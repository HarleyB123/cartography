import logging
from typing import Any
from typing import Dict
from typing import List

import boto3
import neo4j

from cartography.util import aws_handle_regions
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)


@timeit
@aws_handle_regions
def get_eks_clusters(boto3_session: boto3.session.Session, region: str) -> List[Dict]:
    client = boto3_session.client('eks', region_name=region)
    clusters: List[Dict] = []
    paginator = client.get_paginator('list_clusters')
    for page in paginator.paginate():
        clusters.extend(page['clusters'])
    return clusters


@timeit
@aws_handle_regions
def get_eks_nodegroups(boto3_session: boto3.session.Session, region: str, cluster_name) -> List[Dict]:
    client = boto3_session.client('eks', region_name=region)
    nodegroups: List[Dict] = []
    paginator = client.get_paginator('list_nodegroups')
    operation_parameters = {'clusterName': cluster_name}
    for page in paginator.paginate(**operation_parameters):
        nodegroups.extend(page['nodegroups'])
    return nodegroups


@timeit
def get_eks_describe_nodegroup(boto3_session: boto3.session.Session, region: str, cluster_name: str, nodegroup_name: str) -> Dict:
    client = boto3_session.client('eks', region_name=region)
    response = client.describe_nodegroup(clusterName=cluster_name, nodegroupName=nodegroup_name)
    return response['nodegroup']


@timeit
def get_eks_describe_cluster(boto3_session: boto3.session.Session, region: str, cluster_name: str) -> Dict:
    client = boto3_session.client('eks', region_name=region)
    response = client.describe_cluster(name=cluster_name)
    return response['cluster']


@timeit
def load_eks_clusters(
    neo4j_session: neo4j.Session, cluster_data: Dict, region: str, current_aws_account_id: str,
    aws_update_tag: int,
) -> None:
    query: str = """
    MERGE (cluster:EKSCluster{id: {ClusterArn}})
    ON CREATE SET cluster.firstseen = timestamp(),
                cluster.arn = {ClusterArn},
                cluster.name = {ClusterName},
                cluster.region = {Region},
                cluster.created_at = {CreatedAt}
    SET cluster.lastupdated = {aws_update_tag},
        cluster.endpoint = {ClusterEndpoint},
        cluster.endpoint_public_access = {ClusterEndointPublic},
        cluster.rolearn = {ClusterRoleArn},
        cluster.version = {ClusterVersion},
        cluster.platform_version = {ClusterPlatformVersion},
        cluster.status = {ClusterStatus},
        cluster.audit_logging = {ClusterLogging}
    WITH cluster
    MATCH (owner:AWSAccount{id: {AWS_ACCOUNT_ID}})
    MERGE (owner)-[r:RESOURCE]->(cluster)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """

    for cd in cluster_data:
        cluster = cluster_data[cd]
        neo4j_session.run(
            query,
            ClusterArn=cluster['arn'],
            ClusterName=cluster['name'],
            ClusterEndpoint=cluster.get('endpoint'),
            ClusterEndointPublic=cluster.get('resourcesVpcConfig', {}).get('endpointPublicAccess'),
            ClusterRoleArn=cluster.get('roleArn'),
            ClusterVersion=cluster.get('version'),
            ClusterPlatformVersion=cluster.get('platformVersion'),
            ClusterStatus=cluster.get('status'),
            CreatedAt=str(cluster.get('createdAt')),
            ClusterLogging=_process_logging(cluster),
            Region=region,
            aws_update_tag=aws_update_tag,
            AWS_ACCOUNT_ID=current_aws_account_id,
        )


@timeit
def load_eks_nodegroups(
    neo4j_session: neo4j.Session, nodegroup_data: Dict, region: str, current_aws_account_id: str,
    aws_update_tag: int,
) -> None:
    query: str = """
    MERGE (nodegroup:EKSNodeGroup{arn: {GroupArn}})
    ON CREATE SET nodegroup.firstseen = timestamp(),
                nodegroup.arn = {GroupArn},
                nodegroup.name = {GroupName},
                nodegroup.cluster_name = {ClusterName},
                nodegroup.region = {Region},
                nodegroup.created_at = {GroupCreatedAt}
    SET nodegroup.lastupdated = {aws_update_tag},
        nodegroup.version = {GroupVersion},
        nodegroup.status = {GroupStatus}
    WITH nodegroup
    MATCH (owner:AWSAccount{id: {AWS_ACCOUNT_ID}})
    MERGE (owner)-[r:RESOURCE]->(nodegroup)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """

    for nd in nodegroup_data:
        nodegroup = nodegroup_data[nd]
        print(nodegroup.get('clusterName'))
        neo4j_session.run(
            query,
            GroupArn=nodegroup['nodegroupArn'],
            GroupName=nodegroup['nodegroupName'],
            ClusterName=nodegroup.get('clusterName'),
            GroupVersion=nodegroup.get('version'),
            GroupStatus=nodegroup.get('status'),
            GroupCreatedAt=str(nodegroup.get('createdAt')),
            Region=region,
            aws_update_tag=aws_update_tag,
            AWS_ACCOUNT_ID=current_aws_account_id,
        )


def _process_logging(cluster: Dict) -> bool:
    """
    Parse cluster.logging.clusterLogging to verify if
    at least one entry has audit logging set to Enabled.
    """
    logging: bool = False
    cluster_logging: Any = cluster.get('logging', {}).get('clusterLogging')
    if cluster_logging:
        logging = any(filter(lambda x: 'audit' in x['types'] and x['enabled'], cluster_logging))  # type: ignore
    return logging


@timeit
def cleanup(neo4j_session: neo4j.Session, common_job_parameters: Dict) -> None:
    run_cleanup_job('aws_import_eks_cleanup.json', neo4j_session, common_job_parameters)


@timeit
def sync(
    neo4j_session: neo4j.Session, boto3_session: boto3.session.Session, regions: List[str], current_aws_account_id: str,
    update_tag: int, common_job_parameters: Dict,
) -> None:
    for region in regions:
        logger.info("Syncing EKS for region '%s' in account '%s'.", region, current_aws_account_id)

        clusters: List[Dict] = get_eks_clusters(boto3_session, region)

        cluster_data: Dict = {}
        nodegroup_data: Dict = {}
        for cluster_name in clusters:
            nodegroups: List[Dict] = get_eks_nodegroups(boto3_session, region, cluster_name)
            cluster_data[cluster_name] = get_eks_describe_cluster(boto3_session, region, cluster_name)
            for nodegroup_name in nodegroups:
                nodegroup_data[nodegroup_name] = get_eks_describe_nodegroup(boto3_session, region, cluster_name, nodegroup_name)

        load_eks_nodegroups(neo4j_session, nodegroup_data, region, current_aws_account_id, update_tag)
        load_eks_clusters(neo4j_session, cluster_data, region, current_aws_account_id, update_tag)

    #cleanup(neo4j_session, common_job_parameters)

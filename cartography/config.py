class Config:
    """
    A common interface for cartography configuration.

    All fields defined on this class must be present on a configuration object. Fields documented as required must
    contain valid values. Fields documented as optional may contain None, in which case cartography will choose a
    sensible default value for that piece of configuration.

    :type neo4j_uri: string
    :param neo4j_uri: URI for a Neo4j graph database service. Required.
    :type neo4j_user: string
    :param neo4j_user: User name for a Neo4j graph database service. Optional.
    :type neo4j_password: string
    :param neo4j_password: Password for a Neo4j graph database service. Optional.
    :type neo4j_max_connection_lifetime: int
    :param neo4j_max_connection_lifetime: Time in seconds for Neo4j driver to consider a TCP connection alive.
        See https://neo4j.com/docs/driver-manual/1.7/client-applications/. Optional.
    :type update_tag: int
    :param update_tag: Update tag for a cartography sync run. Optional.
    :type aws_sync_all_profiles: bool
    :param aws_sync_all_profiles: If True, AWS sync will run for all non-default profiles in the AWS_CONFIG_FILE. If
        False (default), AWS sync will run using the default credentials only. Optional.
    :type azure_sync_all_subscriptions: bool
    :param azure_sync_all_subscriptions: If True, Azure sync will run for all profiles in azureProfile.json. If
        False (default), Azure sync will run using current user session via CLI credentials. Optional.
    :type azure_sp_auth: bool
    :param azure_sp_auth: If True, Azure sync will run using Service Principal Authentication. If
        False (default), Azure sync will run using current user session via CLI credentials. Optional.
    :type azure_tenant_id: str
    :param azure_tenant_id: Tenant Id for connecting in a Service Principal Authentication approach. Optional.
    :type azure_client_id: str
    :param azure_client_id: Client Id for connecting in a Service Principal Authentication approach. Optional.
    :type azure_client_secret: str
    :param azure_client_secret: Client Secret for connecting in a Service Principal Authentication approach. Optional.
    :type aws_requested_syncs: str
    :param aws_requested_syncs: Comma-separated list of AWS resources to sync. Optional.
    :type crxcavator_api_base_uri: str
    :param crxcavator_api_base_uri: URI for CRXcavator API. Optional.
    :type crxcavator_api_key: str
    :param crxcavator_api_key: Auth key for CRXcavator API. Optional.
    :type analysis_job_directory: str
    :param analysis_job_directory: Path to a directory tree containing analysis jobs to run. Optional.
    :type okta_org_id: str
    :param okta_org_id: Okta organization id. Optional.
    :type okta_api_key: str
    :param okta_api_key: Okta API key. Optional.
    :type okta_saml_role_regex: str
    :param okta_saml_role_regex: The regex used to map okta groups to AWS roles. Optional.
    :type github_config: str
    :param github_config: Base64 encoded config object for GitHub ingestion. Optional.
    :type digitalocean_token: str
    :param digitalocean_token: DigitalOcean access token. Optional.
    :type permission_relationships_file: str
    :param permission_relationships_file: File path for the resource permission relationships file. Optional.
    :type jamf_base_uri: string
    :param jamf_base_uri: Jamf data provider base URI, e.g. https://example.com/JSSResource. Optional.
    :type jamf_user: string
    :param jamf_user: User name used to authenticate to the Jamf data provider. Optional.
    :type jamf_password: string
    :param jamf_password: Password used to authenticate to the Jamf data provider. Optional.
    :type statsd_enabled: bool
    :param statsd_enabled: Whether to collect statsd metrics such as sync execution times. Optional.
    :type statsd_host: str
    :param statsd_host: If statsd_enabled is True, send metrics to this host. Optional.
    :type: statsd_port: int
    :param statsd_port: If statsd_enabled is True, send metrics to this port on statsd_host. Optional.
    :type: k8s_kubeconfig: str
    :param k8s_kubeconfig: Path to kubeconfig file for kubernetes cluster(s). Optional
    :type: pagerduty_api_key: str
    :param pagerduty_api_key: API authentication key for pagerduty. Optional.
    """

    def __init__(
        self,
        neo4j_uri,
        neo4j_user=None,
        neo4j_password=None,
        neo4j_max_connection_lifetime=None,
        update_tag=None,
        aws_sync_all_profiles=False,
        azure_sync_all_subscriptions=False,
        azure_sp_auth=None,
        azure_tenant_id=None,
        azure_client_id=None,
        azure_client_secret=None,
        aws_requested_syncs=None,
        analysis_job_directory=None,
        crxcavator_api_base_uri=None,
        crxcavator_api_key=None,
        okta_org_id=None,
        okta_api_key=None,
        okta_saml_role_regex=None,
        github_config=None,
        digitalocean_token=None,
        permission_relationships_file=None,
        jamf_base_uri=None,
        jamf_user=None,
        jamf_password=None,
        k8s_kubeconfig=None,
        statsd_enabled=False,
        statsd_prefix=None,
        statsd_host=None,
        statsd_port=None,
        pagerduty_api_key=None,
    ):
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.neo4j_max_connection_lifetime = neo4j_max_connection_lifetime
        self.update_tag = update_tag
        self.aws_sync_all_profiles = aws_sync_all_profiles
        self.azure_sync_all_subscriptions = azure_sync_all_subscriptions
        self.azure_sp_auth = azure_sp_auth
        self.azure_tenant_id = azure_tenant_id
        self.azure_client_id = azure_client_id
        self.azure_client_secret = azure_client_secret
        self.aws_requested_syncs = aws_requested_syncs
        self.analysis_job_directory = analysis_job_directory
        self.crxcavator_api_base_uri = crxcavator_api_base_uri
        self.crxcavator_api_key = crxcavator_api_key
        self.okta_org_id = okta_org_id
        self.okta_api_key = okta_api_key
        self.okta_saml_role_regex = okta_saml_role_regex
        self.github_config = github_config
        self.digitalocean_token = digitalocean_token
        self.permission_relationships_file = permission_relationships_file
        self.jamf_base_uri = jamf_base_uri
        self.jamf_user = jamf_user
        self.jamf_password = jamf_password
        self.k8s_kubeconfig = k8s_kubeconfig
        self.statsd_enabled = statsd_enabled
        self.statsd_prefix = statsd_prefix
        self.statsd_host = statsd_host
        self.statsd_port = statsd_port
        self.pagerduty_api_key = pagerduty_api_key

package app

// Tool names. They are shared by registration, per-cluster tool filters, and hints.
const (
	ToolListClusters          = "list_clusters"
	ToolDescribeCluster       = "describe_cluster"
	ToolListTopics            = "list_topics"
	ToolDescribeTopic         = "describe_topic"
	ToolListConsumerGroups    = "list_consumer_groups"
	ToolDescribeConsumerGroup = "describe_consumer_group"
	ToolSampleMessages        = "sample_messages"
	ToolGetMessages           = "get_messages"
	ToolInferSchema           = "infer_schema"
	ToolValidateFilter        = "validate_filter"
	ToolScanMessages          = "scan_messages"
	ToolCountMessages         = "count_messages"
	ToolGetJob                = "get_job"
	ToolListJobs              = "list_jobs"
	ToolCancelJob             = "cancel_job"
	ToolSaveScan              = "save_scan"
	ToolListSavedScans        = "list_saved_scans"
	ToolRunSavedScan          = "run_saved_scan"
	ToolDeleteSavedScan       = "delete_saved_scan"
	ToolListSchemaSubjects    = "list_schema_subjects"
	ToolGetSchema             = "get_schema"
	ToolProduceMessage        = "produce_message"
)

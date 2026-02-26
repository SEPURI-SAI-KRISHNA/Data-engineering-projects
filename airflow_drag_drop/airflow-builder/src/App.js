import React, { useState, useCallback, useRef, useMemo } from 'react';
import ReactFlow, {
  ReactFlowProvider,
  addEdge,
  useNodesState,
  useEdgesState,
  Controls,
  Background,
  Handle,
  Position,
  useReactFlow,
  BaseEdge,
  getBezierPath,
  EdgeLabelRenderer
} from 'reactflow';
import 'reactflow/dist/style.css';
import yaml from 'js-yaml';
import './dnd.css';

// ==========================================
// 1. COMPONENT DEFINITIONS
// ==========================================

const CustomEdge = ({ id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, style = {}, markerEnd }) => {
  const { deleteElements } = useReactFlow();
  const [edgePath, labelX, labelY] = getBezierPath({ sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition });
  return (
    <>
      <BaseEdge path={edgePath} markerEnd={markerEnd} style={style} />
      <EdgeLabelRenderer>
        <div style={{ position: 'absolute', transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`, pointerEvents: 'all' }} className="nodrag nopan">
          <button className="edgebutton" onClick={(e) => { e.stopPropagation(); deleteElements({ edges: [{ id }] }); }}>×</button>
        </div>
      </EdgeLabelRenderer>
    </>
  );
};

const NodeHeader = ({ label, icon, onDelete, typeClass }) => (
  <div className={`node-header ${typeClass}`}>
    <div className="header-left">
      <div className="icon-wrapper">{icon}</div>
      <span className="node-title">{label}</span>
    </div>
    <button className="delete-node-btn" onClick={onDelete}>×</button>
  </div>
);

// --- Nodes ---
const BashNode = ({ id, data, isConnectable }) => {
  const { deleteElements } = useReactFlow();
  return (
    <div className="custom-node bash-node">
      <NodeHeader label="Bash" icon="_>" onDelete={(e) => { e.stopPropagation(); deleteElements({ nodes: [{ id }] }); }} typeClass="bash-header" />
      <div className="node-body"><div className="node-label">{data.label}</div><div className="node-meta">{data.bash_command?.substring(0, 15)}...</div></div>
      <Handle type="target" position={Position.Top} isConnectable={isConnectable} className="custom-handle input-handle" />
      <Handle type="source" position={Position.Bottom} isConnectable={isConnectable} className="custom-handle output-handle" />
    </div>
  );
};

const PythonNode = ({ id, data, isConnectable }) => {
  const { deleteElements } = useReactFlow();
  return (
    <div className="custom-node python-node">
      <NodeHeader label="Python" icon="🐍" onDelete={(e) => { e.stopPropagation(); deleteElements({ nodes: [{ id }] }); }} typeClass="python-header" />
      <div className="node-body"><div className="node-label">{data.label}</div><div className="node-meta">{data.python_callable_name}</div></div>
      <Handle type="target" position={Position.Top} isConnectable={isConnectable} className="custom-handle input-handle" />
      <Handle type="source" position={Position.Bottom} isConnectable={isConnectable} className="custom-handle output-handle" />
    </div>
  );
};

const HttpNode = ({ id, data, isConnectable }) => {
  const { deleteElements } = useReactFlow();
  return (
    <div className="custom-node http-node">
      <NodeHeader label="HTTP" icon="🌐" onDelete={(e) => { e.stopPropagation(); deleteElements({ nodes: [{ id }] }); }} typeClass="http-header" />
      <div className="node-body"><div className="node-label">{data.label}</div><div className="node-meta">{data.method}</div></div>
      <Handle type="target" position={Position.Top} isConnectable={isConnectable} className="custom-handle input-handle" />
      <Handle type="source" position={Position.Bottom} isConnectable={isConnectable} className="custom-handle output-handle" />
    </div>
  );
};

const EmrNode = ({ id, data, isConnectable }) => {
  const { deleteElements } = useReactFlow();
  return (
    <div className="custom-node emr-node">
      <NodeHeader label="EMR Job" icon="☁️" onDelete={(e) => { e.stopPropagation(); deleteElements({ nodes: [{ id }] }); }} typeClass="emr-header" />
      <div className="node-body"><div className="node-label">{data.label}</div><div className="node-meta">{data.name}</div></div>
      <Handle type="target" position={Position.Top} isConnectable={isConnectable} className="custom-handle input-handle" />
      <Handle type="source" position={Position.Bottom} isConnectable={isConnectable} className="custom-handle output-handle" />
    </div>
  );
};

const DummyNode = ({ id, data, isConnectable }) => {
  const { deleteElements } = useReactFlow();
  return (
    <div className="custom-node dummy-node">
      <NodeHeader label="Dummy" icon="⚪" onDelete={(e) => { e.stopPropagation(); deleteElements({ nodes: [{ id }] }); }} typeClass="dummy-header" />
      <div className="node-body"><div className="node-label">{data.label}</div></div>
      <Handle type="target" position={Position.Top} isConnectable={isConnectable} className="custom-handle input-handle" />
      <Handle type="source" position={Position.Bottom} isConnectable={isConnectable} className="custom-handle output-handle" />
    </div>
  );
};

const DagHeadNode = ({ id, data, isConnectable }) => {
  const { deleteElements } = useReactFlow();
  return (
    <div className="custom-node dag-head-node">
      <NodeHeader label="DAG Config" icon="⚙️" onDelete={(e) => { e.stopPropagation(); deleteElements({ nodes: [{ id }] }); }} typeClass="dag-header" />
      <div className="node-body"><div className="node-label">{data.dag_id}</div><div className="node-meta">{data.schedule_interval}</div></div>
      <Handle type="source" position={Position.Bottom} isConnectable={isConnectable} className="custom-handle output-handle" />
    </div>
  );
};

// ==========================================
// 2. MAIN APP
// ==========================================
let id = 0;
const getId = () => `task_${id++}`;

const DnDFlow = () => {
  const reactFlowWrapper = useRef(null);
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [reactFlowInstance, setReactFlowInstance] = useState(null);
  const [generatedYaml, setGeneratedYaml] = useState('');

  // UI State
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [modalOpen, setModalOpen] = useState(false);
  const [selectedNode, setSelectedNode] = useState(null);
  const [activeTab, setActiveTab] = useState('dag'); // 'dag' or 'default_args'
  const [tempData, setTempData] = useState({});

  const nodeTypes = useMemo(() => ({ bashNode: BashNode, pythonNode: PythonNode, httpNode: HttpNode, emrNode: EmrNode, dummyNode: DummyNode, dagHead: DagHeadNode }), []);
  const edgeTypes = useMemo(() => ({ customEdge: CustomEdge }), []);

  const onDragOver = useCallback((event) => { event.preventDefault(); event.dataTransfer.dropEffect = 'move'; }, []);

  const onDrop = useCallback((event) => {
      event.preventDefault();
      const type = event.dataTransfer.getData('application/reactflow');
      if (!type) return;

      const position = reactFlowInstance.project({
        x: event.clientX - reactFlowWrapper.current.getBoundingClientRect().left,
        y: event.clientY - reactFlowWrapper.current.getBoundingClientRect().top,
      });

      let newNode = { id: getId(), position };

      if (type === 'dagHead') {
          newNode = {
              ...newNode, type: 'dagHead',
              data: {
                  // DAG Config
                  dag_id: 'my_dag', schedule_interval: '@daily', description: 'An example DAG',
                  start_date: '2024-01-01', end_date: '', catchup: false, max_active_runs: 1,
                  tags: 'example, airflow', dagrun_timeout: '', is_paused_upon_creation: false,
                  // Default Args
                  owner: 'airflow', depends_on_past: false, email: '', email_on_failure: false,
                  email_on_retry: false, retries: 1, retry_delay: '5m', retry_exponential_backoff: false,
                  execution_timeout: '', on_failure_callback: '', on_success_callback: '',
                  on_retry_callback: '', max_active_tis_per_dag: ''
              }
          };
      }
      else if (type === 'BashOperator') { newNode = { ...newNode, type: 'bashNode', data: { label: `bash_${id}`, bash_command: "echo 'hello'", env: '{}', retries: 3, retry_delay_sec: 300 } }; }
      else if (type === 'PythonOperator') { newNode = { ...newNode, type: 'pythonNode', data: { label: `py_${id}`, python_callable_name: 'my_func', python_callable_file: '/opt/script.py', op_args: '[]', op_kwargs: '{}', provide_context: true, retries: 3, retry_delay_sec: 300 } }; }
      else if (type === 'SimpleHttpOperator') { newNode = { ...newNode, type: 'httpNode', data: { label: `http_${id}`, http_conn_id: 'http_default', endpoint: '/api', method: 'GET', data: '', headers: '{}', response_check: '', log_response: true, retries: 3, retry_delay_sec: 300 } }; }
      else if (type === 'EmrServerlessStartJobOperator') {
        newNode = { ...newNode, type: 'emrNode', data: {
            label: `emr_${id}`, name: 'my_spark_job', application_id: '00fn7tephg467o1t', execution_role_arn: 'arn:aws:iam::...', aws_conn_id: 'aws_default',
            enable_application_ui_links: true, deferrable: false, wait_for_completion: true, waiter_delay: 60, waiter_max_attempts: 20,
            entryPoint: 's3://bucket/app.py', entryPointArguments: '[]', sparkSubmitParameters: '--conf spark.executor.cores=1',
            sparkProperties: [{ key: 'spark.executor.memory', value: '4G' }, { key: 'spark.executor.cores', value: '2' }],
            logGroupName: '/aws/emr-serverless', logStreamNamePrefix: 'emr_log', executionTimeoutMinutes: 0
        }};
      }
      else if (type === 'DummyOperator') { newNode = { ...newNode, type: 'dummyNode', data: { label: `start_${id}` } }; }

      setNodes((nds) => nds.concat(newNode));
    }, [reactFlowInstance, setNodes]
  );

  const onConnect = useCallback((params) => setEdges((eds) => addEdge({ ...params, type: 'customEdge', animated: true, style: { stroke: '#94a3b8', strokeWidth: 2 } }, eds)), [setEdges]);

  const onNodeDoubleClick = (event, node) => {
    setSelectedNode(node);
    setTempData(JSON.parse(JSON.stringify(node.data)));
    setActiveTab('dag'); // Reset tab
    setModalOpen(true);
  };

  const saveConfig = () => {
    setNodes((nds) => nds.map((n) => n.id === selectedNode.id ? { ...n, data: { ...tempData } } : n));
    setModalOpen(false);
  };

  // Spark helpers
  const addSparkProperty = () => setTempData(prev => ({ ...prev, sparkProperties: [...(prev.sparkProperties || []), { key: '', value: '' }] }));
  const updateSparkProperty = (index, field, value) => { const newProps = [...tempData.sparkProperties]; newProps[index][field] = value; setTempData({ ...tempData, sparkProperties: newProps }); };
  const removeSparkProperty = (index) => setTempData({ ...tempData, sparkProperties: tempData.sparkProperties.filter((_, i) => i !== index) });

  // --- YAML GENERATOR ---
  const generateYAML = () => {
    const dagHead = nodes.find(n => n.type === 'dagHead');
    const dagId = dagHead ? dagHead.data.dag_id : 'generated_dag';

    // Process DAG Head Data
    let dagProps = {};
    let defaultArgs = { owner: 'airflow', start_date: '2023-01-01', retries: 1 }; // Fallback defaults

    if (dagHead) {
        const d = dagHead.data;

        // 1. Top Level DAG Props
        dagProps = {
            schedule_interval: d.schedule_interval,
            start_date: d.start_date,
            end_date: d.end_date || null,
            catchup: d.catchup,
            max_active_runs: parseInt(d.max_active_runs) || 1,
            dagrun_timeout: d.dagrun_timeout || null,
            description: d.description,
            is_paused_upon_creation: d.is_paused_upon_creation,
            tags: d.tags ? d.tags.split(',').map(t => t.trim()) : []
        };

        // 2. Default Args
        defaultArgs = {
            owner: d.owner,
            depends_on_past: d.depends_on_past,
            email: d.email ? d.email.split(',').map(e => e.trim()) : [],
            email_on_failure: d.email_on_failure,
            email_on_retry: d.email_on_retry,
            retries: parseInt(d.retries),
            retry_delay: d.retry_delay, // Keep as string (e.g. "5m") or timedelta
            retry_exponential_backoff: d.retry_exponential_backoff,
            execution_timeout: d.execution_timeout || null,
            on_failure_callback: d.on_failure_callback || null,
            on_success_callback: d.on_success_callback || null,
            on_retry_callback: d.on_retry_callback || null,
            max_active_tis_per_dag: d.max_active_tis_per_dag ? parseInt(d.max_active_tis_per_dag) : null
        };
    }

    let dagConfig = {
        [dagId]: {
            default_args: defaultArgs,
            ...dagProps,
            tasks: {}
        }
    };

    const MARKER = "___FLOW_MARKER___";
    const asFlow = (obj) => `${MARKER}${JSON.stringify(obj)}${MARKER}`;

    nodes.filter(n => n.type !== 'dagHead').forEach(node => {
      let taskConfig = {};
      const d = node.data;

      if (node.type === 'bashNode') {
          taskConfig = { operator: 'airflow.operators.bash.BashOperator', bash_command: d.bash_command, env: asFlow(JSON.parse(d.env || '{}')), cwd: d.cwd || null, retries: parseInt(d.retries), retry_delay_sec: parseInt(d.retry_delay_sec) };
      }
      else if (node.type === 'pythonNode') {
          taskConfig = { operator: 'airflow.operators.python.PythonOperator', python_callable_name: d.python_callable_name, python_callable_file: d.python_callable_file, op_args: JSON.parse(d.op_args || '[]'), op_kwargs: asFlow(JSON.parse(d.op_kwargs || '{}')), provide_context: d.provide_context, retries: parseInt(d.retries), retry_delay_sec: parseInt(d.retry_delay_sec) };
      }
      else if (node.type === 'httpNode') {
          taskConfig = { operator: 'airflow.providers.http.operators.http.SimpleHttpOperator', http_conn_id: d.http_conn_id, endpoint: d.endpoint, method: d.method, data: d.data || null, headers: asFlow(JSON.parse(d.headers || '{}')), response_check: d.response_check || null, log_response: d.log_response, retries: parseInt(d.retries), retry_delay_sec: parseInt(d.retry_delay_sec) };
      }
      else if (node.type === 'emrNode') {
          const sparkPropsObj = {};
          (d.sparkProperties || []).forEach(p => { if(p.key) sparkPropsObj[p.key] = p.value; });
          const jobDriver = { sparkSubmit: { entryPoint: d.entryPoint, entryPointArguments: JSON.parse(d.entryPointArguments || '[]'), sparkSubmitParameters: d.sparkSubmitParameters } };
          const configOverrides = { applicationConfiguration: [{ classification: 'spark-defaults', properties: sparkPropsObj }], monitoringConfiguration: { cloudWatchLoggingConfiguration: { enabled: true, logGroupName: d.logGroupName, logStreamNamePrefix: d.logStreamNamePrefix, logTypes: { SPARK_DRIVER: ['STDERR', 'STDOUT'], SPARK_EXECUTOR: ['STDERR', 'STDOUT'] } } } };
          const configMap = { executionTimeoutMinutes: parseInt(d.executionTimeoutMinutes || 0) };

          taskConfig = {
              operator: 'airflow.providers.amazon.aws.operators.emr.EmrServerlessStartJobOperator', name: d.name, application_id: d.application_id, execution_role_arn: d.execution_role_arn, aws_conn_id: d.aws_conn_id, enable_application_ui_links: d.enable_application_ui_links, deferrable: d.deferrable, wait_for_completion: d.wait_for_completion, waiter_delay: parseInt(d.waiter_delay), waiter_max_attempts: parseInt(d.waiter_max_attempts),
              job_driver: asFlow(jobDriver), configuration_overrides: asFlow(configOverrides), config: asFlow(configMap)
          };
      }
      else if (node.type === 'dummyNode') { taskConfig = { operator: 'airflow.operators.dummy.DummyOperator' }; }

      dagConfig[dagId].tasks[d.label] = taskConfig;
      const upstreamIds = edges.filter(e => e.target === node.id).map(e => { const src = nodes.find(n => n.id === e.source); return (src && src.type !== 'dagHead') ? src.data.label : null; }).filter(Boolean);
      if (upstreamIds.length > 0) { dagConfig[dagId].tasks[d.label]['dependencies'] = upstreamIds; }
    });

    const rawYaml = yaml.dump(dagConfig, { lineWidth: -1, noRefs: true });
    const regex = /['"]?___FLOW_MARKER___(.+?)___FLOW_MARKER___['"]?/g;
    const cleanYaml = rawYaml.replace(regex, (match, jsonString) => jsonString.replace(/\\"/g, '"'));
    setGeneratedYaml(cleanYaml);
  };

  const renderModalContent = () => {
    if (!selectedNode) return null;
    const t = tempData;
    const type = selectedNode.type;

    // --- DAG HEAD (Tabs for Complex Config) ---
    if (type === 'dagHead') {
        return (
            <div className="modal-scroll-area">
                <div className="tab-header">
                    <button className={`tab-btn ${activeTab === 'dag' ? 'active' : ''}`} onClick={() => setActiveTab('dag')}>DAG Config</button>
                    <button className={`tab-btn ${activeTab === 'default_args' ? 'active' : ''}`} onClick={() => setActiveTab('default_args')}>Default Args</button>
                </div>

                {activeTab === 'dag' ? (
                    <div className="tab-content">
                        <div className="form-group"><label>DAG ID</label><input value={t.dag_id} onChange={(e) => setTempData({...t, dag_id: e.target.value})} /></div>
                        <div className="form-group"><label>Description</label><input value={t.description} onChange={(e) => setTempData({...t, description: e.target.value})} /></div>
                        <div className="row"><div className="form-group half"><label>Schedule</label><input value={t.schedule_interval} onChange={(e) => setTempData({...t, schedule_interval: e.target.value})} /></div><div className="form-group half"><label>Timezone</label><input placeholder="UTC" disabled /></div></div>
                        <div className="row"><div className="form-group half"><label>Start Date</label><input type="datetime-local" value={t.start_date} onChange={(e) => setTempData({...t, start_date: e.target.value})} /></div><div className="form-group half"><label>End Date</label><input type="datetime-local" value={t.end_date} onChange={(e) => setTempData({...t, end_date: e.target.value})} /></div></div>
                        <div className="form-group"><label>Tags (comma separated)</label><input value={t.tags} onChange={(e) => setTempData({...t, tags: e.target.value})} /></div>
                        <div className="row"><div className="form-group half"><label>Max Active Runs</label><input type="number" value={t.max_active_runs} onChange={(e) => setTempData({...t, max_active_runs: e.target.value})} /></div><div className="form-group half"><label>DAG Timeout</label><input value={t.dagrun_timeout} onChange={(e) => setTempData({...t, dagrun_timeout: e.target.value})} /></div></div>
                        <div className="row checkbox-row"><div className="checkbox-group"><input type="checkbox" checked={t.catchup} onChange={(e) => setTempData({...t, catchup: e.target.checked})} /><label>Catchup</label></div><div className="checkbox-group"><input type="checkbox" checked={t.is_paused_upon_creation} onChange={(e) => setTempData({...t, is_paused_upon_creation: e.target.checked})} /><label>Paused on Create</label></div></div>
                    </div>
                ) : (
                    <div className="tab-content">
                        <div className="form-group"><label>Owner</label><input value={t.owner} onChange={(e) => setTempData({...t, owner: e.target.value})} /></div>
                        <div className="form-group"><label>Emails (comma separated)</label><input value={t.email} onChange={(e) => setTempData({...t, email: e.target.value})} /></div>
                        <div className="row checkbox-row"><div className="checkbox-group"><input type="checkbox" checked={t.email_on_failure} onChange={(e) => setTempData({...t, email_on_failure: e.target.checked})} /><label>Email on Failure</label></div><div className="checkbox-group"><input type="checkbox" checked={t.email_on_retry} onChange={(e) => setTempData({...t, email_on_retry: e.target.checked})} /><label>Email on Retry</label></div></div>
                        <div className="row"><div className="form-group half"><label>Retries</label><input type="number" value={t.retries} onChange={(e) => setTempData({...t, retries: e.target.value})} /></div><div className="form-group half"><label>Retry Delay</label><input value={t.retry_delay} onChange={(e) => setTempData({...t, retry_delay: e.target.value})} /></div></div>
                        <div className="checkbox-group"><input type="checkbox" checked={t.retry_exponential_backoff} onChange={(e) => setTempData({...t, retry_exponential_backoff: e.target.checked})} /><label>Exponential Backoff</label></div>
                        <div className="form-section"><label className="section-label">Callbacks</label><div className="form-group"><label>On Failure</label><input value={t.on_failure_callback} onChange={(e) => setTempData({...t, on_failure_callback: e.target.value})} /></div><div className="form-group"><label>On Success</label><input value={t.on_success_callback} onChange={(e) => setTempData({...t, on_success_callback: e.target.value})} /></div><div className="form-group"><label>On Retry</label><input value={t.on_retry_callback} onChange={(e) => setTempData({...t, on_retry_callback: e.target.value})} /></div></div>
                        <div className="form-group"><label>Exec Timeout</label><input value={t.execution_timeout} onChange={(e) => setTempData({...t, execution_timeout: e.target.value})} /></div>
                        <div className="checkbox-group"><input type="checkbox" checked={t.depends_on_past} onChange={(e) => setTempData({...t, depends_on_past: e.target.checked})} /><label>Depends on Past</label></div>
                    </div>
                )}
            </div>
        );
    }

    if (type === 'dummyNode') return <div className="form-group"><label>Task ID</label><input value={t.label} onChange={(e) => setTempData({...t, label: e.target.value})} /></div>;

    return (
        <div className="modal-scroll-area">
            <div className="form-section"><label className="section-label">General Info</label><div className="form-group"><label>Task ID</label><input value={t.label} onChange={(e) => setTempData({...t, label: e.target.value})} /></div></div>

            {type === 'bashNode' && (<div className="form-section"><label className="section-label">Bash Config</label><div className="form-group"><label>Command</label><input className="code-input" value={t.bash_command} onChange={(e) => setTempData({...t, bash_command: e.target.value})} /></div><div className="form-group"><label>CWD</label><input value={t.cwd} onChange={(e) => setTempData({...t, cwd: e.target.value})} /></div><div className="form-group"><label>Env (JSON)</label><input className="code-input" value={t.env} onChange={(e) => setTempData({...t, env: e.target.value})} /></div></div>)}

            {type === 'pythonNode' && (<div className="form-section"><label className="section-label">Python Config</label><div className="form-group"><label>Callable Name</label><input value={t.python_callable_name} onChange={(e) => setTempData({...t, python_callable_name: e.target.value})} /></div><div className="form-group"><label>File Path</label><input value={t.python_callable_file} onChange={(e) => setTempData({...t, python_callable_file: e.target.value})} /></div><div className="form-group"><label>Args (List)</label><input className="code-input" value={t.op_args} onChange={(e) => setTempData({...t, op_args: e.target.value})} /></div><div className="form-group"><label>Kwargs (Dict)</label><input className="code-input" value={t.op_kwargs} onChange={(e) => setTempData({...t, op_kwargs: e.target.value})} /></div><div className="checkbox-group"><input type="checkbox" checked={t.provide_context} onChange={(e) => setTempData({...t, provide_context: e.target.checked})} /><label>Provide Context</label></div></div>)}

            {type === 'httpNode' && (<div className="form-section"><label className="section-label">Request Config</label><div className="row"><div className="form-group half"><label>Conn ID</label><input value={t.http_conn_id} onChange={(e) => setTempData({...t, http_conn_id: e.target.value})} /></div><div className="form-group half"><label>Method</label><select value={t.method} onChange={(e) => setTempData({...t, method: e.target.value})}><option>GET</option><option>POST</option><option>PUT</option><option>DELETE</option></select></div></div><div className="form-group"><label>Endpoint</label><input value={t.endpoint} onChange={(e) => setTempData({...t, endpoint: e.target.value})} /></div><div className="form-group"><label>Data</label><textarea className="code-input" rows={3} value={t.data} onChange={(e) => setTempData({...t, data: e.target.value})} /></div><div className="form-group"><label>Headers (JSON)</label><input className="code-input" value={t.headers} onChange={(e) => setTempData({...t, headers: e.target.value})} /></div><div className="checkbox-group"><input type="checkbox" checked={t.log_response} onChange={(e) => setTempData({...t, log_response: e.target.checked})} /><label>Log Response</label></div></div>)}

            {type === 'emrNode' && (
                <>
                <div className="form-section"><label className="section-label">Job Config</label><div className="row"><div className="form-group half"><label>Job Name</label><input value={t.name} onChange={(e) => setTempData({...t, name: e.target.value})} /></div><div className="form-group half"><label>App ID</label><input value={t.application_id} onChange={(e) => setTempData({...t, application_id: e.target.value})} /></div></div><div className="form-group"><label>Execution Role ARN</label><input value={t.execution_role_arn} onChange={(e) => setTempData({...t, execution_role_arn: e.target.value})} /></div><div className="form-group"><label>AWS Conn ID</label><input value={t.aws_conn_id} onChange={(e) => setTempData({...t, aws_conn_id: e.target.value})} /></div><div className="row checkbox-row"><div className="checkbox-group"><input type="checkbox" checked={t.enable_application_ui_links} onChange={(e) => setTempData({...t, enable_application_ui_links: e.target.checked})} /><label>UI Links</label></div><div className="checkbox-group"><input type="checkbox" checked={t.deferrable} onChange={(e) => setTempData({...t, deferrable: e.target.checked})} /><label>Deferrable</label></div></div></div>
                <div className="form-section"><label className="section-label">Waiters</label><div className="checkbox-group"><input type="checkbox" checked={t.wait_for_completion} onChange={(e) => setTempData({...t, wait_for_completion: e.target.checked})} /><label>Wait for Completion</label></div><div className="row"><div className="form-group half"><label>Delay (s)</label><input type="number" value={t.waiter_delay} onChange={(e) => setTempData({...t, waiter_delay: e.target.value})} /></div><div className="form-group half"><label>Attempts</label><input type="number" value={t.waiter_max_attempts} onChange={(e) => setTempData({...t, waiter_max_attempts: e.target.value})} /></div></div><div className="form-group"><label>Timeout (min)</label><input type="number" value={t.executionTimeoutMinutes} onChange={(e) => setTempData({...t, executionTimeoutMinutes: e.target.value})} /></div></div>
                <div className="form-section"><label className="section-label">Spark Submit</label><div className="form-group"><label>Entry Point (S3)</label><input value={t.entryPoint} onChange={(e) => setTempData({...t, entryPoint: e.target.value})} /></div><div className="form-group"><label>Args (List)</label><input className="code-input" value={t.entryPointArguments} onChange={(e) => setTempData({...t, entryPointArguments: e.target.value})} /></div><div className="form-group"><label>Spark Params</label><input value={t.sparkSubmitParameters} onChange={(e) => setTempData({...t, sparkSubmitParameters: e.target.value})} /></div></div>
                <div className="form-section"><label className="section-label">Spark Defaults</label>{t.sparkProperties.map((prop, idx) => (<div key={idx} className="kv-row"><input placeholder="Key" value={prop.key} onChange={(e) => updateSparkProperty(idx, 'key', e.target.value)} /><input placeholder="Value" value={prop.value} onChange={(e) => updateSparkProperty(idx, 'value', e.target.value)} /><button className="remove-btn" onClick={() => removeSparkProperty(idx)}>×</button></div>))}<button className="add-btn" onClick={addSparkProperty}>+ Add Property</button></div>
                <div className="form-section"><label className="section-label">Logging</label><div className="row"><div className="form-group half"><label>Log Group</label><input value={t.logGroupName} onChange={(e) => setTempData({...t, logGroupName: e.target.value})} /></div><div className="form-group half"><label>Prefix</label><input value={t.logStreamNamePrefix} onChange={(e) => setTempData({...t, logStreamNamePrefix: e.target.value})} /></div></div></div>
                </>
            )}

            <div className="form-section"><label className="section-label">Retries</label><div className="row"><div className="form-group half"><label style={{fontSize: '0.8rem', color: '#94a3b8'}}>Count</label><input type="number" value={t.retries} onChange={(e) => setTempData({...t, retries: e.target.value})} /></div><div className="form-group half"><label style={{fontSize: '0.8rem', color: '#94a3b8'}}>Delay (sec)</label><input type="number" value={t.retry_delay_sec} onChange={(e) => setTempData({...t, retry_delay_sec: e.target.value})} /></div></div></div>
        </div>
    );
  };

  return (
    <div className="dndflow dark-theme">
      <ReactFlowProvider>
        <aside className={`sidebar ${sidebarOpen ? 'open' : 'closed'}`}>
          <button className="toggle-btn" onClick={() => setSidebarOpen(!sidebarOpen)}>{sidebarOpen ? '<<' : '>>'}</button>
          {sidebarOpen && (<><div className="logo-area"><h3>Airflow Builder</h3></div><div className="tools-grid"><div className="tool-item dag-tool" onDragStart={(e) => e.dataTransfer.setData('application/reactflow', 'dagHead')} draggable><div className="tool-icon">⚙️</div><span>DAG Config</span></div><div className="tool-item" onDragStart={(e) => e.dataTransfer.setData('application/reactflow', 'BashOperator')} draggable><div className="tool-icon">_></div><span>Bash</span></div><div className="tool-item" onDragStart={(e) => e.dataTransfer.setData('application/reactflow', 'PythonOperator')} draggable><div className="tool-icon">🐍</div><span>Python</span></div><div className="tool-item" onDragStart={(e) => e.dataTransfer.setData('application/reactflow', 'SimpleHttpOperator')} draggable><div className="tool-icon">🌐</div><span>HTTP</span></div><div className="tool-item emr-tool" onDragStart={(e) => e.dataTransfer.setData('application/reactflow', 'EmrServerlessStartJobOperator')} draggable><div className="tool-icon">☁️</div><span>EMR Serverless</span></div><div className="tool-item" onDragStart={(e) => e.dataTransfer.setData('application/reactflow', 'DummyOperator')} draggable><div className="tool-icon">⚪</div><span>Dummy</span></div></div><button className="generate-btn" onClick={generateYAML}>Generate YAML</button>{generatedYaml && <div className="yaml-preview"><pre>{generatedYaml}</pre></div>}</>)}
        </aside>
        <div className="reactflow-wrapper" ref={reactFlowWrapper}><ReactFlow nodes={nodes} edges={edges} onNodesChange={onNodesChange} onEdgesChange={onEdgesChange} onConnect={onConnect} onInit={setReactFlowInstance} onDrop={onDrop} onDragOver={onDragOver} onNodeDoubleClick={onNodeDoubleClick} nodeTypes={nodeTypes} edgeTypes={edgeTypes} fitView><Controls className="custom-controls" /><Background color="#111" gap={16} size={1} /></ReactFlow></div>
        {modalOpen && (<div className="modal-overlay"><div className="modal-content"><div className="modal-header"><h4>Configure {selectedNode.type.replace('Node', '')}</h4><button className="close-btn" onClick={() => setModalOpen(false)}>×</button></div>{renderModalContent()}<div className="modal-footer"><button className="secondary-btn" onClick={() => setModalOpen(false)}>Cancel</button><button className="primary-btn" onClick={saveConfig}>Save Changes</button></div></div></div>)}
      </ReactFlowProvider>
    </div>
  );
};
export default DnDFlow;
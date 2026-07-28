# Airflow Drag-and-Drop DAG Builder

A browser-based visual builder for Apache Airflow DAGs. Drag operator nodes onto a canvas, connect them with edges to define execution order, configure each node's parameters in a side-panel, then export the finished pipeline as an Airflow-compatible YAML definition — no hand-written DAG code required.

## What it does

- **Drag-and-drop canvas** — built on [React Flow](https://reactflow.dev/); nodes can be freely repositioned and reconnected.
- **Operator nodes** — currently includes `BashOperator` and other common Airflow operator types. Each node exposes the relevant parameters as inline form fields.
- **Edge management** — click the × button on any edge or node to remove it; connections are drawn as Bézier curves with a delete affordance.
- **YAML export** — the toolbar serialises the current graph into a structured YAML representation of the DAG (tasks, dependencies, and parameters).

## Directory layout

```
airflow_drag_drop/
└── airflow-builder/         # React application (Create React App)
    ├── src/
    │   ├── App.js           # Main canvas, node definitions, toolbar, export logic
    │   ├── dnd.css          # Drag-and-drop and node styling
    │   └── index.js         # React entry point
    ├── public/
    ├── package.json
    └── README.md            # CRA default scripts reference
```

## Quick start

```bash
cd airflow_drag_drop/airflow-builder
npm install
npm start
```

The app opens at `http://localhost:3000`.

To build a production bundle:

```bash
npm run build
```

## How it fits into this repository

The DAG builder is a developer-productivity tool in the collection. It addresses the authoring side of Airflow pipelines — the step before code is committed — and complements the data-processing projects (streaming lakehouse, fraud detection, etc.) by providing a visual way to design the orchestration layer around them.

---

> Part of [Data Engineering Projects](../README.md) · [Contributing](../CONTRIBUTING.md) · [MIT License](../LICENSE)

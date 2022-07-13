exports.Mapper = class Mapper {
  constructor(mappingMode) {
    this.mappingMode = mappingMode;
  }
  mapNeptuneEdge(edge) {
    return {
      id: typeof edge['id'] !== "string" ? JSON.stringify(edge['id']) : edge['id'],
      from: edge['from'],
      to: edge['to'],
      label: edge['label'],
      properties: edge['properties'],
    };
  }

  mapNeptuneNode(node) {
    return {
      id: node['id'],
      label: node['label'],
      properties: node['properties'],
      edges: []
    };
  }

  mapGremlinNode(node) {
    return {
      id: node.get('id'),
      label: node.get('label'),
      properties: mapToObj(node.get('properties')),
      edges: edgesToJson(node.get('edges'), 'gremlin')
    };
  }

  mapGremlinEdge(edge) {
    return {
      id: typeof edge.get('id') !== "string" ? JSON.stringify(edge.get('id')) : edge.get('id'),
      from: edge.get('from'),
      to: edge.get('to'),
      label: edge.get('label'),
      properties: mapToObj(edge.get('properties')),
    };
  }

  edgesToJson(edgeList) {
    return edgeList.map(edge => this.mappingMode == 'neptune' ? this.mapNeptuneEdge(edge) : this.mapGremlinEdge(edge));
  }

  nodesToJson(nodeList) {
    return nodeList.map(node => this.mappingMode == 'neptune' ? this.mapNeptuneNode(node) : this.mapGremlinNode(node));
  }
};


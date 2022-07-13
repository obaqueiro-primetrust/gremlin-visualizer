const gremlin = require('gremlin');
const gremlinSV4 = require('gremlin-aws-sigv4');
const { Mapper } = require('./mapper');

// Override error handler function FROM gremlin-aws-sigv4 to avoid throwing an exception that cannot be caught
class AWSConnection extends gremlinSV4.driver.AwsSigV4DriverRemoteConnection {
  _errorHandler(error) {
    try {
      super._errorHandler(error);
    } catch(e) {
      return;
    }
  }
}

function shuffleArrayInPlace(array) {
  for (let i = array.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [array[i], array[j]] = [array[j], array[i]];
  }
}

exports.QueryProcessor = class QueryProcessor {
  constructor(req, res, next) {
    this.reqId = req.id;
    this.res = res;
    this.next = next;
    this.req = req;

    this.warnings = [];
    this.gremlinHost = this.req.body.host;
    this.gremlinPort = this.req.body.port;
    this.nodeLimit = this.req.body.nodeLimit || 100;
    this.query = this.req.body.query;
    this.hostMode = '';
    if (this.gremlinHost.includes('neptune.amazonaws.com')) {
      this.hostMode = 'neptune';
    }    
  } 

  log(text) {
    console.log(`[${this.reqId}] `+ text)
  }

  /**
   * Adds the useDFE AWS Neptune side effect setting to a Gremlin query
   * See: https://docs.aws.amazon.com/neptune/latest/userguide/gremlin-query-hints-useDFE.html
   *  
   */ 
  addDFEtoQuery(query) {
    query = query.split('.')
    query[0] = "g.withSideEffect('Neptune#useDFE', true)"
    return query.join('.')
  }

  getVerticesQuery(query) {
    // Force Neptune DFE use if querying AWS neptune
    query = this.hostMode == 'neptune' ? this.addDFEtoQuery(query) : query

    // 1 main query getting all nodes
    return `${query}.dedup().project('id', 'label', 'properties').by(` +
      "__.id()).by(__.label()).by(__.valueMap().by(__.unfold()))"
  }

  getVertexEdges(vertexId) {
    let query = 'g'
    query = this.hostMode == 'neptune' ? this.addDFEtoQuery(query) : query
    return `${query}.V(${JSON.stringify(vertexId)}).bothE().id()`;
  }

  getEdgeDataQuery(edgeId) {
    let query = 'g'
    query = this.hostMode == 'neptune' ?  this.addDFEtoQuery(query) : query
    return `${query}.E(${JSON.stringify(edgeId)}).project('id', 'from', 'to', 'label', 'properties'` +
    ").by(__.id()).by(__.outV().id()).by(__.inV().id()).by(__.label()" +
    ").by(__.valueMap()).by(__.bothE().project('id').by(__.id()).fold())"
  }

   processEdges(vertex,edgesList) {    
    return this.connection.submit(this.getEdgeDataQuery(edgesList.shift())).then(edgeData => {
      let edges = new Mapper(this.hostMode).edgesToJson(edgeData.traversers)
      vertex.edges.push(...edges)
      if (edgesList.length > 0) 
        return this.processEdges(vertex, edgesList);
    })
  }

  processVertices(vertices) {
    if (vertices.length == 0) {
      this.log("No vertices found for this query")
      this.res.send([]);
      return;
    }

    this.log(`Fetching data for ${vertices.length} vertices`)            
        
    // extract/expand edge data
    const promises = vertices.map(vertex => {
      // get edges list for each vertex.edges
      let promise = this.connection.submit(this.getVertexEdges(vertex.id)).then(
        result => {
          let edges = result.traversers;
          if (edges.length == 0) { return; }

          if (edges.length > this.nodeLimit) {
            let warningText = `Vertex ${vertex.id}: Number of edges (${edges.length}) higher than nodeLimit(${
              this.nodeLimit}). Returning a random sample of ${this.nodeLimit} elements`
            this.warnings.push(warningText)
            this.log(warningText)
            shuffleArrayInPlace(edges);
            edges = edges.slice(0, edges);
          }

          this.log(`Fetching data for ${edges.length} edges`)
          return Promise.all(edges.map(edge => {
            return this.connection.submit(this.getEdgeDataQuery(edge)).then(edgeData => {
              let edgesDetail = new Mapper(this.hostMode).edgesToJson(edgeData.traversers)
              vertex.edges.push(...edgesDetail);
            });
          }));
        }
      )
      return promise;
    })

    // Wait until we've got data for all vertices
    Promise.all(promises).then(_ => {
      this.log(`resolved all promises successfully`);
      this.res.send(vertices);
    })
  }

  performQuery() { 
    this.log(`Performing query:${this.query}`);  
    if (this.hostMode == 'neptune') {      
      this.performNeptuneQuery();
    }
    else {
      performGremlinQuery();
    }
  }

  performGremlinQuery() {
    const url = `ws://${gremlinHost}:${gremlinPort}/gremlin`;
      const client = new gremlin.driver.Client(url, { traversalSource: 'g', mimeType: 'application/json' });
      client.submit(makeQuery(query, this.nodeLimit), {})
        .then((result) => res.send(new Mapper(this.hostMode).nodesToJson(result._items)))
        .catch((err) => { this.log(err); this.next(err) })
  }

  performNeptuneQuery() {
    this.connection = new AWSConnection(
      this.gremlinHost, this.gremlinPort, { secure: true, autoReconnect: true },
      // connected callback
      () => {
        let q = this.getVerticesQuery(this.query, this.hostMode);
        this.connection.submit(q,).then(r => {
          let vertices = new Mapper(this.hostMode).nodesToJson(r.traversers)
          if (vertices.length > this.nodeLimit) {
            let warningText = `Query: Number of vertices (${vertices.length}) higher than nodeLimit(
              ${this.nodeLimit}). Returning a random sample of ${this.nodeLimit} elements.`
            this.log(warningText)              
              shuffleArrayInPlace(vertices);
              vertices = vertices.slice(0,this.nodeLimit);
          }                    
          this.processVertices(vertices);
             
        }).catch(err => {
          this.log("Error submitting request:")
          this.log(err);
          this.next(err);
        });
      }, 
    );
  }
}

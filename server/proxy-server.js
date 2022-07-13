const express = require('express');
const bodyParser = require('body-parser');
const gremlin = require('gremlin');
const cors = require('cors');
const app = express();
const port = 3001;
const gremlinSV4 = require('gremlin-aws-sigv4');
const { OAuth2Client } = require('google-auth-library');
const uuid =  require('uuid');


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

app.use(cors({
  credentials: true,
}));

// parse application/json
app.use(bodyParser.json());

function mapToObj(inputMap) {
  let obj = {};
  inputMap.forEach((value, key) => {
    obj[key] = value
  });

  return obj;
}

class Mapper {
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
    }
  }

  mapNeptuneNode(node) {
    return {
      id: node['id'],
      label: node['label'],
      properties: node['properties'],
      edges: []
    }
  }

  mapGremlinNode(node) {
    return {
      id: node.get('id'),
      label: node.get('label'),
      properties: mapToObj(node.get('properties')),
      edges: edgesToJson(node.get('edges'), 'gremlin')
    }
  }

  mapGremlinEdge(edge) {
    return {
      id: typeof edge.get('id') !== "string" ? JSON.stringify(edge.get('id')) : edge.get('id'),
      from: edge.get('from'),
      to: edge.get('to'),
      label: edge.get('label'),
      properties: mapToObj(edge.get('properties')),
    }
  }

  edgesToJson(edgeList) {  
    return edgeList.map( edge => this.mappingMode == 'neptune' ? this.mapNeptuneEdge(edge) : this.mapGremlinEdge(edge));
  }

  nodesToJson(nodeList) {
    return nodeList.map( node =>this.mappingMode == 'neptune' ? this.mapNeptuneNode(node) : this.mapGremlinNode(node));
  }
}

function makeQuery(query, nodeLimit) {
  const nodeLimitQuery = !isNaN(nodeLimit) && Number(nodeLimit) > 0 ? `.limit(${nodeLimit})`: '';
  return `${query}${nodeLimitQuery}.dedup().as('node').project('id', 'label', 'properties', 'edges').by(__.id()).by(__.label()).by(__.valueMap().by(__.unfold())).by(__.outE().project('id', 'from', 'to', 'label', 'properties').by(__.id()).by(__.select('node').id()).by(__.inV().id()).by(__.label()).by(__.valueMap().by(__.unfold())).fold())`;
}

class QueryProcessor {
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

  // Adds the useDFE side effect setting to a Gremlin query
  addDFEtoQuery(query) {
    query = query.split('.')
    query[0] = "g.withSideEffect('Neptune#useDFE', true)"
    return query.join('.')
  }

  getVerticesQuery(query) {
    // Force Neptune DFE use if querying AWS neptune
    query = this.hostMode == 'neptune' ? this.addDFEtoQuery(query) : query

    // 1 main query getting all nodes
    return `${query}.limit(${this.nodeLimit}).dedup().project('id', 'label', 'properties').by(` +
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
    const promises = [];
    
    // extract/expand edge data
    for (let vertex of vertices) {                
      // get edges list for each vertex.edges
      let promise = this.connection.submit(this.getVertexEdges(vertex.id)).then(
        result => {
          let edges = result.traversers;
          if (edges.length > this.nodeLimit) {
            let warningText = `Vertex ${vertex.id}: Number of edges (${edges.length}) higher than nodeLimit(${
              this.nodeLimit}). Returning a random sample of ${this.nodeLimit} elements`
            this.warnings.push(warningText)
            this.log(warningText)              
              shuffleArrayInPlace(edges);
              edges = edges.slice(0,edges);
          }   
          if (edges.length > 0) {
            this.log(`Fetching data for ${edges.length} edges`)            
            return Promise.all(edges.map(edge=> { 
              return this.connection.submit(this.getEdgeDataQuery(edge)).then(edgeData => {
                let edgesDetail = new Mapper(this.hostMode).edgesToJson(edgeData.traversers)
                vertex.edges.push(...edgesDetail)
              })  
            }));
          }
        }
      )
      promises.push(promise);
    }
  
    
    Promise.all(promises).then(_=> {
      this.log(`resolved all promises successfully`);            
      this.res.send(vertices);
    })
  }

  performQuery() {
 
    this.log(`Performing query:${this.query}`);
  
    if (this.hostMode == 'neptune') {      
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
    else {
      const url = `ws://${gremlinHost}:${gremlinPort}/gremlin`;
      const client = new gremlin.driver.Client(url, { traversalSource: 'g', mimeType: 'application/json' });
      client.submit(makeQuery(query, this.nodeLimit), {})
        .then((result) => res.send(new Mapper(this.hostMode).nodesToJson(result._items)))
        .catch((err) => { this.log(err); this.next(err) })
    }
  }
}


function checkAuthentication(auth) {
  return new Promise((resolve, _) => {
    // Resole to ture (authenticate) if there's no Google Client ID 
    if (!process.env.GOOGLE_CLIENT_ID)  {
      resolve(true);
      return;
    }

    let googleSession = JSON.parse(auth);
    if (!googleSession.clientId || !googleSession.credential) {
      resolve(false);
      return;
    }
    // Using our own google client ID ensures that we *only* accept Google Id tokens that
    // are valid for our application
    const client = new OAuth2Client(process.env.GOOGLE_CLIENT_ID);

    client.verifyIdToken({
      idToken: googleSession.credential,
      audience: process.env.GOOGLE_CLIENT_ID}).then(ticket => {
        resolve(true);
        return;
      }).catch(err=> {
        resolve(false)
      })
  })
}



function shuffleArrayInPlace(array) {
  for (let i = array.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [array[i], array[j]] = [array[j], array[i]];
  }
}



app.post('/query', (req, res, next) => {
  req.id = uuid.v4();
  checkAuthentication(req.body.auth).then(isAuth => {
    if (!isAuth)  {
      res.sendStatus(401);    
    }
    else {
      new QueryProcessor(req, res, next).performQuery();
    }
  });

});

// Makes the app less brittle so that it doesn't crash when there's a timeout or a request error
app.on('uncaughtException', (err) => {
  console.log('Error while processing request')
  console.log(err);
  next(err)
})

app.listen(port, () => console.log(`Simple gremlin-proxy server listening on port ${port}!`));

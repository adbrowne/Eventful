namespace Eventful.Neo4j

/// Unique within a graph.
type NodeId = string

type Relationship =
    { From : NodeId
      To : NodeId
      Type : string }

type GraphAction =
    | AddRelationship of Relationship
    | RemoveRelationship of Relationship
    | RemoveAllIncomingRelationships of NodeId * relationshipType : string
    | AddLabels of NodeId * Set<string>
    | UpdateNode of NodeId * data : obj

type GraphTransaction = GraphTransaction of GraphAction seq

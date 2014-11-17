namespace Eventful.Neo4j

type NodeId =
    { Label : string
      Id : string }

type Relationship =
    { From : NodeId
      To : NodeId
      Type : string }

type GraphAction =
    | AddRelationship of Relationship
    | RemoveRelationship of Relationship
    | RemoveAllIncomingRelationships of NodeId * relationshipType : string
    | UpdateNode of NodeId * data : obj

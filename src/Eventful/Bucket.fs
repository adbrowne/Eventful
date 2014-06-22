namespace Eventful

module Bucket =
   let getBucket buckets (x:obj) =
     let objHash = hash x
     abs <| objHash % buckets
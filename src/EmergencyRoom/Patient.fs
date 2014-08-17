namespace EmergencyRoom

open System

type PatientId = { Id : Guid } 
     with 
     static member New () = 
        { 
            Id = (Guid.NewGuid()) 
        }
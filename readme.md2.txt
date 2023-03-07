Adding to the above.

The logic to check for overlapping blocks during block setup service would be validated for that sequence type and not for the application, sequence type combination 

Problem Statement :
 
“What happens if the current block is exhausted when the numbers till upper limit are generated.  How to ensure that the Service doesn’t need any code changes or manual intervention”
 
Solution
 
Below is the record information for Number Generator control table –
 
Application Name : IMPACT       (other values could be -  SURETY / MGA)
Sequence Type      : Account      (other values could be – Policy …)
StartNumber          : 30000001
EndNumber            : 80000000
LastNumber            : 38471234 – Incremented which each number generation request for this application/sequence combination
 
New Attribute being Added
ActiveFlag                : true or Y
 
When the service is called, the service will have parameters as Application Name and Sequence Number, using which it reads the above control table, increments the LastNumber by One and returns the number ( as well as logging the request and generated number to log table).  This service would also check if the Active Flag = Y or true.
 
While incrementing the number, if the lastnumber reaches the endnumber, the activeflag is set as N or false.   On the subsequent request, the servie would not find any active record for that requested parameters and hence return an error message stating that the block is exhausted, need to create a new block.
 
There would be another service which can be used by the admin which is called with parameters ( application name, sequence name, startnumber and endnumber ).  This request would be validated is this block is free from any overlapping, if yes then this record is inserted to the control table and marked active flag = Y, lastnumber=startnumber for this one
 
Next time when the number generating service is called, it finds this active block and starts returning the incremented number
 
With this approach, in future any range can be defined and as needed any other sequence series can also be defined for any application…e.g. Quote Number generation by PC. Claim Number generator for CC and so on…
 

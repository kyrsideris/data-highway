--------------------- MODULE offramp-v2-service-high-level ---------------------
EXTENDS Integers
VARIABLES requested, state, event

Init == (state = "start") /\ (requested = 0) /\ (event = "-")

Connected == \/ /\ state = "start"
                /\ state' = "connected"
                /\ UNCHANGED <<event, requested>>

ReceivedEvent == \/ /\ state = "connected"
                    /\ event' \in {"REQUEST", "CANCEL", "COMMIT"}
                    /\ state' = "event"
                    /\ UNCHANGED <<requested>>

NoEvent == \/ /\ state = "connected"
              /\ requested > 0
              /\ state' = "messaging"
              /\ UNCHANGED <<event>>

SendMessage == \/ /\ state = "messaging"
                  /\ requested' = requested - 1
                  /\ state' = "connected"
                  /\ UNCHANGED <<event>>

UpdateRequest == [n \in 1..2000 |-> requested + n]

RequestEvent ==  \/ /\ state = "event"
                    /\ event = "REQUEST"
                    /\ requested' = UpdateRequest
                    /\ state' = "connected"

CancelEvent ==  \/ /\ state = "event"
                   /\ event = "CANCEL"
                   /\ requested' = 0
                   /\ state' = "connected"

CommitEvent ==  \/ /\ state = "event"
                   /\ event = "COMMIT"
                   /\ state' = "connected"
                   /\ UNCHANGED <<requested>>

Next == Connected \/ ReceivedEvent \/ NoEvent

=============================================================================

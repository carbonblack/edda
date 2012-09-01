package com.netflix.edda

import scala.actors.Actor

object Queryable extends StateMachine.LocalState[CollectionState] {
    private case class Query(from: Actor, query: Map[String,Any], limit: Int, live: Boolean) extends StateMachine.Message
    private case class QueryResult(from: Actor, records: Seq[Record]) extends StateMachine.Message {
        override def toString = "QueryResult(records=" + records.size +")";
    }
}

abstract class Queryable extends Observable {
    import Queryable._

    def query(queryMap: Map[String,Any], limit: Int=0, live: Boolean = false): Seq[Record] = {
        val self = this
        this !? Query(this,queryMap,limit,live) match {
            case QueryResult(`self`,results) => results
        }
    }

    protected
    def doQuery(queryMap: Map[String,Any], limit: Int, live: Boolean, state: StateMachine.State): Seq[Record]

    protected
    def firstOf(limit: Int, records: Seq[Record]): Seq[Record] = {
        if( limit > 0 ) records.take(limit) else records
    }
    private
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (Query(from,queryMap,limit,live),state) => {
            val replyTo = sender
            Utils.NamedActor(this + " Query processor") {
                replyTo ! QueryResult(this,doQuery(queryMap, limit, live, state))
            }
            state
        }
    }

    override protected
    def transitions = localTransitions orElse super.transitions
}
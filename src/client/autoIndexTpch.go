package client

import (
	"fmt"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	antidote "potionDB/potionDB/components"
)

//The purpose of this file is to expose the internals of the tpch package to autoindex.

const (
	CONNECTIONLESS = false //When true, methods return dummy values instead of contacting the servers.
)

type AutoIndexTPCH struct {
}

func (tpch AutoIndexTPCH) PrepareCommunications() {
	if !CONNECTIONLESS {
		fmt.Println("Servers: ", servers)
		//servers = []string{"localhost:8087"}
		//servers = []string{"potiondb8087:8087"}
		//conns = make([]net.Conn, 1)
		//channels.dataChans = []chan QueuedMsg{make(chan QueuedMsg, MAX_BUFF_PROTOS)}
		connectToServers()
		fmt.Println("[INFO]Connected to:", servers)
	} else {
		fmt.Println("[INFO]Client in connectionless mode.")
	}
}

func (tpch AutoIndexTPCH) DoGet(readParams []crdt.ReadObjectParams) *proto.ApbReadObjectResp {
	if !CONNECTIONLESS {
		antidote.SendProto(antidote.StaticReadObjs, antidote.CreateStaticReadObjs(nil, readParams), conns[0])
		_, tmpProto, _ := antidote.ReceiveProto(conns[0])
		return tmpProto.(*proto.ApbStaticReadObjectsResp).Objects.Objects[0]
	}
	return nil
}

func (tpch AutoIndexTPCH) DoUpdate(updParams []crdt.UpdateObjectParams) {
	if !CONNECTIONLESS {
		antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(nil, updParams), conns[0])
		antidote.ReceiveProto(conns[0])
	}
}

func (tpch AutoIndexTPCH) SendTrigger(trigger antidote.AutoUpdate, isGeneric bool, ci antidote.CodingInfo) {
	if !CONNECTIONLESS {
		antidote.SendProto(antidote.NewTrigger, antidote.CreateNewTrigger(trigger, isGeneric, ci), conns[0])
		antidote.ReceiveProto(conns[0])
	}
}

func (tpch AutoIndexTPCH) GetTriggers() {
	if !CONNECTIONLESS {
		antidote.SendProto(antidote.GetTriggers, antidote.CreateGetTriggers(), conns[0])
		_, msg, _ := antidote.ReceiveProto(conns[0])
		convertedProto := msg.(*proto.ApbGetTriggersReply)
		fmt.Printf("%v\n%v\n", convertedProto.Mapping, convertedProto.GenericMapping)
	}
}

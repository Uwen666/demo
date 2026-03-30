package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/shard"
)

type ReputationRelayOutsideModule struct {
	relayMod *RawRelayOutsideModule
	repMod   *ReputationSupervisionMod
}

func NewReputationRelayOutsideModule(p *PbftConsensusNode) *ReputationRelayOutsideModule {
	return NewReputationRelayOutsideModuleWithRepMod(p, NewReputationSupervisionMod(p))
}

func NewReputationRelayOutsideModuleWithRepMod(p *PbftConsensusNode, repMod *ReputationSupervisionMod) *ReputationRelayOutsideModule {
	return &ReputationRelayOutsideModule{
		relayMod: &RawRelayOutsideModule{pbftNode: p},
		repMod:   repMod,
	}
}

func (m *ReputationRelayOutsideModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CRelay, message.CRelayWithProof, message.CInject:
		if m.repMod.pbftNode.RunningNode.NodeType == shard.NodeType_Storage {
			if msgType == message.CInject {
				m.repMod.pbftNode.pl.Plog.Printf("S%dN%d : storage role ignored injected txs\n", m.repMod.pbftNode.ShardID, m.repMod.pbftNode.NodeID)
			}
			return true
		}
		return m.relayMod.HandleMessageOutsidePBFT(msgType, content)
	default:
		return m.repMod.HandleMessageOutsidePBFT(msgType, content)
	}
}

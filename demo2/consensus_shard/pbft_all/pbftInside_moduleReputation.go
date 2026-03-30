package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/shard"
)

// ReputationPbftInsideExtraHandleMod bridges PBFT stages with the supervision
// logic so state request / TB upload hooks are executed in Reputation mode.
type ReputationPbftInsideExtraHandleMod struct {
	relayMod *RawRelayPbftExtraHandleMod
	repMod   *ReputationSupervisionMod
}

func NewReputationPbftInsideExtraHandleMod(p *PbftConsensusNode, repMod *ReputationSupervisionMod) *ReputationPbftInsideExtraHandleMod {
	return &ReputationPbftInsideExtraHandleMod{
		relayMod: &RawRelayPbftExtraHandleMod{pbftNode: p},
		repMod:   repMod,
	}
}

func (m *ReputationPbftInsideExtraHandleMod) HandleinPropose() (bool, *message.Request) {
	return m.relayMod.HandleinPropose()
}

func (m *ReputationPbftInsideExtraHandleMod) HandleinPrePrepare(ppmsg *message.PrePrepare) bool {
	m.repMod.HandlePrePrepare(ppmsg)
	if m.repMod.pbftNode.RunningNode.NodeType != shard.NodeType_Consensus {
		// Storage nodes should not participate in PBFT voting.
		return false
	}
	return m.relayMod.HandleinPrePrepare(ppmsg)
}

func (m *ReputationPbftInsideExtraHandleMod) HandleinPrepare(pmsg *message.Prepare) bool {
	return m.relayMod.HandleinPrepare(pmsg)
}

func (m *ReputationPbftInsideExtraHandleMod) HandleinCommit(cmsg *message.Commit) bool {
	ok := m.relayMod.HandleinCommit(cmsg)
	m.repMod.HandleCommit(cmsg)
	return ok
}

func (m *ReputationPbftInsideExtraHandleMod) HandleReqestforOldSeq(rom *message.RequestOldMessage) bool {
	return m.relayMod.HandleReqestforOldSeq(rom)
}

func (m *ReputationPbftInsideExtraHandleMod) HandleforSequentialRequest(som *message.SendOldMessage) bool {
	return m.relayMod.HandleforSequentialRequest(som)
}

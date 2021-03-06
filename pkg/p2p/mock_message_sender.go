package p2p

import (
	"context"
	"sync"

	"github.com/edwingeng/deque"
)

// MockMessageSender defines a mock message sender
type MockMessageSender struct {
	mu        sync.Mutex
	msgBox    map[msgBoxIndex]deque.Deque
	isBlocked bool

	injectedErrCh chan error
}

// NewMockMessageSender creates a new MockMessageSender instance
func NewMockMessageSender() *MockMessageSender {
	return &MockMessageSender{
		msgBox:        make(map[msgBoxIndex]deque.Deque),
		injectedErrCh: make(chan error, 1),
	}
}

type msgBoxIndex struct {
	topic  Topic
	target NodeID
}

// SendToNodeB implements pkg/p2p.MessageSender.SendToNodeB
func (m *MockMessageSender) SendToNodeB(
	ctx context.Context,
	targetNodeID NodeID,
	topic Topic,
	message interface{},
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case err := <-m.injectedErrCh:
		return err
	default:
	}

	// TODO Handle the `m.isBlocked == true` case
	q := m.getQueue(targetNodeID, topic)
	q.PushBack(message)
	return nil
}

// SendToNode implements pkg/p2p.MessageSender.SendToNode
func (m *MockMessageSender) SendToNode(
	_ context.Context,
	targetNodeID NodeID,
	topic Topic,
	message interface{},
) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case err := <-m.injectedErrCh:
		return false, err
	default:
	}

	if m.isBlocked {
		return false, nil
	}

	q := m.getQueue(targetNodeID, topic)
	q.PushBack(message)

	return true, nil
}

// TryPop tries to get a message from message sender
func (m *MockMessageSender) TryPop(targetNodeID NodeID, topic Topic) (interface{}, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	q := m.getQueue(targetNodeID, topic)
	if q.Empty() {
		return nil, false
	}

	return q.PopFront(), true
}

func (m *MockMessageSender) getQueue(target NodeID, topic Topic) deque.Deque {
	mapKey := msgBoxIndex{
		topic:  topic,
		target: target,
	}

	q, ok := m.msgBox[mapKey]
	if !ok {
		q = deque.NewDeque()
		m.msgBox[mapKey] = q
	}

	return q
}

// SetBlocked makes the message send blocking
func (m *MockMessageSender) SetBlocked(isBlocked bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.isBlocked = isBlocked
}

// InjectError injects error to simulate error scenario
func (m *MockMessageSender) InjectError(err error) {
	m.injectedErrCh <- err
}

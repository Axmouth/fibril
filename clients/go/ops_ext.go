package fibril

// Cluster, declaration, and stream ops: the second half of the wire surface.
// Same byte-exact rules as ops.go, pinned by clients/wire_vectors.json.

// ---- shared composite types --------------------------------------------

// AdvertisedAddress is a broker endpoint the client can connect to, with
// optional routing tags.
type AdvertisedAddress struct {
	Host string
	Port uint16
	Tags []string
}

func (w *writer) advertisedAddresses(addrs []AdvertisedAddress) {
	w.u32(uint32(len(addrs)))
	for _, a := range addrs {
		w.writeStr(a.Host)
		w.u16(a.Port)
		w.u32(uint32(len(a.Tags)))
		for _, t := range a.Tags {
			w.writeStr(t)
		}
	}
}

func (r *reader) advertisedAddresses() []AdvertisedAddress {
	n := r.u32()
	out := make([]AdvertisedAddress, 0, n)
	for i := uint32(0); i < n && r.err == nil; i++ {
		a := AdvertisedAddress{Host: r.readStr(), Port: r.u16()}
		m := r.u32()
		for j := uint32(0); j < m && r.err == nil; j++ {
			a.Tags = append(a.Tags, r.readStr())
		}
		out = append(out, a)
	}
	return out
}

func (w *writer) partitionList(ps []uint32) {
	w.u32(uint32(len(ps)))
	for _, p := range ps {
		w.u32(p)
	}
}

func (r *reader) partitionList() []uint32 {
	n := r.u32()
	out := make([]uint32, 0, n)
	for i := uint32(0); i < n && r.err == nil; i++ {
		out = append(out, r.u32())
	}
	return out
}

// ---- declare queue -----------------------------------------------------

// DlqKind is the dead-letter routing policy tag.
type DlqKind uint8

const (
	DlqDiscard DlqKind = 0
	DlqGlobal  DlqKind = 1
	DlqCustom  DlqKind = 2
)

// DlqPolicy is a queue's dead-letter policy. Topic and Group apply only when
// Kind is DlqCustom.
type DlqPolicy struct {
	Kind  DlqKind
	Topic string
	Group *string
}

func (w *writer) optionalDlqPolicy(p *DlqPolicy) {
	if p == nil {
		w.u8(0)
		return
	}
	w.u8(1)
	w.u8(uint8(p.Kind))
	if p.Kind == DlqCustom {
		w.writeStr(p.Topic)
		w.optionalStr(p.Group)
	}
}

func (r *reader) optionalDlqPolicy() *DlqPolicy {
	if r.u8() != 1 {
		return nil
	}
	kind := DlqKind(r.u8())
	if kind > DlqCustom && r.err == nil {
		r.err = &WireError{Kind: WireUnknownTag, Message: "wire: unknown dlq policy"}
		return nil
	}
	p := &DlqPolicy{Kind: kind}
	if kind == DlqCustom {
		p.Topic = r.readStr()
		p.Group = r.optionalStr()
	}
	return p
}

// DeclareQueue declares a queue with optional dead-letter, retry, partition, and
// per-message TTL settings.
type DeclareQueue struct {
	Topic               string
	Group               *string
	DlqPolicy           *DlqPolicy
	DlqMaxRetries       *uint32
	PartitionCount      *uint32
	DefaultMessageTTLms *uint64
}

func EncodeDeclareQueue(d DeclareQueue) []byte {
	w := writer{}
	w.magic("FDQ1")
	w.writeStr(d.Topic)
	w.optionalStr(d.Group)
	w.optionalDlqPolicy(d.DlqPolicy)
	w.optionalU32(d.DlqMaxRetries)
	w.optionalU32(d.PartitionCount)
	// Trailing so a peer that omits it still decodes (read as nil).
	w.optionalU64(d.DefaultMessageTTLms)
	return w.buf
}

func DecodeDeclareQueue(body []byte) (DeclareQueue, error) {
	r := reader{buf: body}
	r.expectMagic("FDQ1")
	d := DeclareQueue{
		Topic:          r.readStr(),
		Group:          r.optionalStr(),
		DlqPolicy:      r.optionalDlqPolicy(),
		DlqMaxRetries:  r.optionalU32(),
		PartitionCount: r.optionalU32(),
	}
	if r.remaining() > 0 {
		d.DefaultMessageTTLms = r.optionalU64()
	}
	return d, r.finish()
}

// DeclareQueueOk is the broker's declare reply.
type DeclareQueueOk struct {
	Status         string
	PartitionCount uint32
}

func EncodeDeclareQueueOk(o DeclareQueueOk) []byte {
	w := writer{}
	w.magic("FDK1")
	w.writeStr(o.Status)
	w.u32(o.PartitionCount)
	return w.buf
}

func DecodeDeclareQueueOk(body []byte) (DeclareQueueOk, error) {
	r := reader{buf: body}
	r.expectMagic("FDK1")
	o := DeclareQueueOk{Status: r.readStr(), PartitionCount: r.u32()}
	return o, r.finish()
}

// ---- declare plexus (fan-out stream) -----------------------------------

// StreamDurability is a Plexus stream's durability tier.
type StreamDurability string

const (
	StreamEphemeral   StreamDurability = "ephemeral"
	StreamSpeculative StreamDurability = "speculative"
	StreamDurable     StreamDurability = "durable"
)

var durabilityOrder = []StreamDurability{StreamEphemeral, StreamSpeculative, StreamDurable}

func (w *writer) durability(d StreamDurability) {
	if d == "" {
		d = StreamDurable // zero value defaults to the durable tier
	}
	for i, v := range durabilityOrder {
		if v == d {
			w.u8(uint8(i))
			return
		}
	}
	w.u8(uint8(len(durabilityOrder) - 1))
}

func (r *reader) durability() StreamDurability {
	tag := r.u8()
	if int(tag) >= len(durabilityOrder) {
		if r.err == nil {
			r.err = &WireError{Kind: WireUnknownTag, Message: "wire: unknown durability"}
		}
		return StreamDurable
	}
	return durabilityOrder[tag]
}

// StreamRetention bounds how much of a stream is retained. Each limit is
// optional (nil = unbounded on that axis).
type StreamRetention struct {
	MaxAgeMs   *uint64
	MaxBytes   *uint64
	MaxRecords *uint64
}

// DeclarePlexus declares a Plexus (fan-out stream) channel.
type DeclarePlexus struct {
	Topic             string
	PartitionCount    *uint32
	Durability        StreamDurability
	Retention         StreamRetention
	ReplicationFactor *uint32
}

func EncodeDeclarePlexus(d DeclarePlexus) []byte {
	w := writer{}
	w.magic("FDP1")
	w.writeStr(d.Topic)
	w.optionalU32(d.PartitionCount)
	w.durability(d.Durability)
	w.optionalU64(d.Retention.MaxAgeMs)
	w.optionalU64(d.Retention.MaxBytes)
	w.optionalU64(d.Retention.MaxRecords)
	w.optionalU32(d.ReplicationFactor)
	return w.buf
}

func DecodeDeclarePlexus(body []byte) (DeclarePlexus, error) {
	r := reader{buf: body}
	r.expectMagic("FDP1")
	d := DeclarePlexus{Topic: r.readStr(), PartitionCount: r.optionalU32(), Durability: r.durability()}
	d.Retention = StreamRetention{MaxAgeMs: r.optionalU64(), MaxBytes: r.optionalU64(), MaxRecords: r.optionalU64()}
	d.ReplicationFactor = r.optionalU32()
	return d, r.finish()
}

// DeclarePlexusOk is the broker's stream-declare reply.
type DeclarePlexusOk struct {
	Status         string
	PartitionCount uint32
}

func EncodeDeclarePlexusOk(o DeclarePlexusOk) []byte {
	w := writer{}
	w.magic("FPK1")
	w.writeStr(o.Status)
	w.u32(o.PartitionCount)
	return w.buf
}

func DecodeDeclarePlexusOk(body []byte) (DeclarePlexusOk, error) {
	r := reader{buf: body}
	r.expectMagic("FPK1")
	o := DeclarePlexusOk{Status: r.readStr(), PartitionCount: r.u32()}
	return o, r.finish()
}

// ---- topology ----------------------------------------------------------

// QueueTopologyEntry is one partition's ownership in the topology.
type QueueTopologyEntry struct {
	Topic               string
	Partition           uint32
	Group               *string
	OwnerEndpoints      []AdvertisedAddress
	PartitioningVersion uint64
	PartitionCount      uint32
}

// StreamTopologyEntry is one stream partition's ownership in the topology.
type StreamTopologyEntry struct {
	Topic               string
	Partition           uint32
	OwnerEndpoints      []AdvertisedAddress
	PartitioningVersion uint64
	PartitionCount      uint32
}

// TopologyOk is a topology snapshot: the ownership of every queue and stream
// partition the broker knows about, at a generation.
type TopologyOk struct {
	Generation uint64
	Queues     []QueueTopologyEntry
	Streams    []StreamTopologyEntry
}

func (w *writer) topologyBody(t TopologyOk) {
	w.u64(t.Generation)
	w.u32(uint32(len(t.Queues)))
	for _, e := range t.Queues {
		w.queueKey(e.Topic, e.Partition, e.Group)
		w.advertisedAddresses(e.OwnerEndpoints)
		w.u64(e.PartitioningVersion)
		w.u32(e.PartitionCount)
	}
	w.u32(uint32(len(t.Streams)))
	for _, s := range t.Streams {
		w.writeStr(s.Topic)
		w.u32(s.Partition)
		w.advertisedAddresses(s.OwnerEndpoints)
		w.u64(s.PartitioningVersion)
		w.u32(s.PartitionCount)
	}
}

func (r *reader) topologyBody() TopologyOk {
	t := TopologyOk{Generation: r.u64()}
	nq := r.u32()
	t.Queues = make([]QueueTopologyEntry, 0, nq)
	for i := uint32(0); i < nq && r.err == nil; i++ {
		e := QueueTopologyEntry{}
		e.Topic, e.Partition, e.Group = r.queueKey()
		e.OwnerEndpoints = r.advertisedAddresses()
		e.PartitioningVersion = r.u64()
		e.PartitionCount = r.u32()
		t.Queues = append(t.Queues, e)
	}
	ns := r.u32()
	t.Streams = make([]StreamTopologyEntry, 0, ns)
	for i := uint32(0); i < ns && r.err == nil; i++ {
		s := StreamTopologyEntry{Topic: r.readStr(), Partition: r.u32()}
		s.OwnerEndpoints = r.advertisedAddresses()
		s.PartitioningVersion = r.u64()
		s.PartitionCount = r.u32()
		t.Streams = append(t.Streams, s)
	}
	return t
}

func EncodeTopologyOk(t TopologyOk) []byte {
	w := writer{}
	w.magic("FTO1")
	w.topologyBody(t)
	return w.buf
}

func DecodeTopologyOk(body []byte) (TopologyOk, error) {
	r := reader{buf: body}
	r.expectMagic("FTO1")
	t := r.topologyBody()
	return t, r.finish()
}

// EncodeTopologyUpdate encodes an unsolicited broker->client topology push: the
// same body as TopologyOk under a distinct magic so a push is distinguishable
// from a request reply.
func EncodeTopologyUpdate(t TopologyOk) []byte {
	w := writer{}
	w.magic("FTU1")
	w.topologyBody(t)
	return w.buf
}

func DecodeTopologyUpdate(body []byte) (TopologyOk, error) {
	r := reader{buf: body}
	r.expectMagic("FTU1")
	t := r.topologyBody()
	return t, r.finish()
}

// TopologyRequest asks for the topology, optionally filtered to one topic/group.
type TopologyRequest struct {
	Topic *string
	Group *string
}

func EncodeTopologyRequest(req TopologyRequest) []byte {
	w := writer{}
	w.magic("FTP1")
	w.optionalStr(req.Topic)
	w.optionalStr(req.Group)
	return w.buf
}

func DecodeTopologyRequest(body []byte) (TopologyRequest, error) {
	r := reader{buf: body}
	r.expectMagic("FTP1")
	req := TopologyRequest{Topic: r.optionalStr(), Group: r.optionalStr()}
	return req, r.finish()
}

// TopologyUpdateAck acknowledges the generation a client now reflects, so the
// broker can fence a repartition cutover.
type TopologyUpdateAck struct {
	Generation uint64
}

func EncodeTopologyUpdateAck(a TopologyUpdateAck) []byte {
	w := writer{}
	w.magic("FTA1")
	w.u64(a.Generation)
	return w.buf
}

func DecodeTopologyUpdateAck(body []byte) (TopologyUpdateAck, error) {
	r := reader{buf: body}
	r.expectMagic("FTA1")
	a := TopologyUpdateAck{Generation: r.u64()}
	return a, r.finish()
}

// ---- reconcile ---------------------------------------------------------

// ReconcilePolicy governs how the broker reconciles a client's subscriptions
// after a reconnect.
type ReconcilePolicy string

const (
	ReconcileConservative ReconcilePolicy = "conservative"
	ReconcileRestore      ReconcilePolicy = "restore_client_subscriptions"
)

func (w *writer) reconcilePolicy(p ReconcilePolicy) {
	if p == ReconcileRestore {
		w.u8(1)
	} else {
		w.u8(0)
	}
}

func (r *reader) reconcilePolicy() ReconcilePolicy {
	if r.u8() == 1 {
		return ReconcileRestore
	}
	return ReconcileConservative
}

// ReconcileSubscription describes a subscription a reconnecting client wants the
// broker to restore.
type ReconcileSubscription struct {
	SubID          uint64
	Topic          string
	Partition      uint32
	Group          *string
	AutoAck        bool
	Prefetch       uint32
	ConsumerGroup  *string
	ConsumerTarget *uint32
	MemberID       *UUID
}

func (w *writer) reconcileSubscription(s ReconcileSubscription) {
	w.u64(s.SubID)
	w.queueKey(s.Topic, s.Partition, s.Group)
	w.writeBool(s.AutoAck)
	w.u32(s.Prefetch)
	w.optionalStr(s.ConsumerGroup)
	w.optionalU32(s.ConsumerTarget)
	w.optionalUUID(s.MemberID)
}

func (r *reader) reconcileSubscription() ReconcileSubscription {
	s := ReconcileSubscription{SubID: r.u64()}
	s.Topic, s.Partition, s.Group = r.queueKey()
	s.AutoAck = r.readBool()
	s.Prefetch = r.u32()
	s.ConsumerGroup = r.optionalStr()
	s.ConsumerTarget = r.optionalU32()
	s.MemberID = r.optionalUUID()
	return s
}

// ReconcileClient asks the broker to reconcile the listed subscriptions under a
// policy after a reconnect.
type ReconcileClient struct {
	Policy        ReconcilePolicy
	Subscriptions []ReconcileSubscription
}

func EncodeReconcileClient(rc ReconcileClient) []byte {
	w := writer{}
	w.magic("FRC1")
	w.reconcilePolicy(rc.Policy)
	w.u32(uint32(len(rc.Subscriptions)))
	for _, s := range rc.Subscriptions {
		w.reconcileSubscription(s)
	}
	return w.buf
}

func DecodeReconcileClient(body []byte) (ReconcileClient, error) {
	r := reader{buf: body}
	r.expectMagic("FRC1")
	rc := ReconcileClient{Policy: r.reconcilePolicy()}
	n := r.u32()
	rc.Subscriptions = make([]ReconcileSubscription, 0, n)
	for i := uint32(0); i < n && r.err == nil; i++ {
		rc.Subscriptions = append(rc.Subscriptions, r.reconcileSubscription())
	}
	return rc, r.finish()
}

// ---- redirect ----------------------------------------------------------

// Redirect tells the client to retry an op against a different owner.
type Redirect struct {
	Topic               string
	Partition           uint32
	Group               *string
	OwnerEndpoints      []AdvertisedAddress
	PartitioningVersion uint64
}

func EncodeRedirect(rd Redirect) []byte {
	w := writer{}
	w.magic("FRD1")
	w.queueKey(rd.Topic, rd.Partition, rd.Group)
	w.advertisedAddresses(rd.OwnerEndpoints)
	w.u64(rd.PartitioningVersion)
	return w.buf
}

func DecodeRedirect(body []byte) (Redirect, error) {
	r := reader{buf: body}
	r.expectMagic("FRD1")
	rd := Redirect{}
	rd.Topic, rd.Partition, rd.Group = r.queueKey()
	rd.OwnerEndpoints = r.advertisedAddresses()
	rd.PartitioningVersion = r.u64()
	return rd, r.finish()
}

// ---- assignment --------------------------------------------------------

// AssignmentChanged notifies an exclusive-cohort member of its new partition
// assignment.
type AssignmentChanged struct {
	Topic         string
	Group         *string
	ConsumerGroup string
	Generation    uint64
	Assigned      []uint32
	Added         []uint32
	Revoked       []uint32
}

func EncodeAssignmentChanged(a AssignmentChanged) []byte {
	w := writer{}
	w.magic("FAC1")
	w.writeStr(a.Topic)
	w.optionalStr(a.Group)
	w.writeStr(a.ConsumerGroup)
	w.u64(a.Generation)
	w.partitionList(a.Assigned)
	w.partitionList(a.Added)
	w.partitionList(a.Revoked)
	return w.buf
}

func DecodeAssignmentChanged(body []byte) (AssignmentChanged, error) {
	r := reader{buf: body}
	r.expectMagic("FAC1")
	a := AssignmentChanged{Topic: r.readStr(), Group: r.optionalStr(), ConsumerGroup: r.readStr(), Generation: r.u64()}
	a.Assigned = r.partitionList()
	a.Added = r.partitionList()
	a.Revoked = r.partitionList()
	return a, r.finish()
}

// ---- going away --------------------------------------------------------

// GoingAway is the broker's drain notice ahead of a planned shutdown or upgrade.
type GoingAway struct {
	GraceMs uint64
	Message string
}

func EncodeGoingAway(g GoingAway) []byte {
	w := writer{}
	w.magic("FGA1")
	w.u64(g.GraceMs)
	w.writeStr(g.Message)
	return w.buf
}

func DecodeGoingAway(body []byte) (GoingAway, error) {
	r := reader{buf: body}
	r.expectMagic("FGA1")
	g := GoingAway{GraceMs: r.u64(), Message: r.readStr()}
	return g, r.finish()
}

// ---- subscribe stream --------------------------------------------------

// StreamStartKind is where a stream subscription begins reading.
type StreamStartKind uint8

const (
	StreamLatest   StreamStartKind = 0
	StreamEarliest StreamStartKind = 1
	StreamOffset   StreamStartKind = 2
	StreamNBack    StreamStartKind = 3
	StreamByTime   StreamStartKind = 4
)

// StreamStart is a stream read start position. Value applies only to the
// offset/nback/bytime kinds.
type StreamStart struct {
	Kind  StreamStartKind
	Value uint64
}

func (w *writer) streamStart(s StreamStart) {
	w.u8(uint8(s.Kind))
	if s.Kind >= StreamOffset {
		w.u64(s.Value)
	}
}

func (r *reader) streamStart() StreamStart {
	kind := StreamStartKind(r.u8())
	if kind > StreamByTime {
		if r.err == nil {
			r.err = &WireError{Kind: WireUnknownTag, Message: "wire: unknown stream start"}
		}
		return StreamStart{}
	}
	s := StreamStart{Kind: kind}
	if kind >= StreamOffset {
		s.Value = r.u64()
	}
	return s
}

// StreamFilter is one header key/pattern predicate on a stream subscription.
// Filters are an ordered list, not a map, so order is significant on the wire.
type StreamFilter struct {
	Key     string
	Pattern string
}

// SubscribeStream requests a Plexus (fan-out stream) subscription of one
// partition.
type SubscribeStream struct {
	Topic       string
	Partition   uint32
	DurableName *string
	Start       StreamStart
	Filter      []StreamFilter
	Prefetch    uint32
	AutoAck     bool
}

func EncodeSubscribeStream(s SubscribeStream) []byte {
	w := writer{}
	w.magic("FSP1")
	w.writeStr(s.Topic)
	w.u32(s.Partition)
	w.optionalStr(s.DurableName)
	w.streamStart(s.Start)
	w.u32(uint32(len(s.Filter)))
	for _, f := range s.Filter {
		w.writeStr(f.Key)
		w.writeStr(f.Pattern)
	}
	w.u32(s.Prefetch)
	w.writeBool(s.AutoAck)
	return w.buf
}

func DecodeSubscribeStream(body []byte) (SubscribeStream, error) {
	r := reader{buf: body}
	r.expectMagic("FSP1")
	s := SubscribeStream{Topic: r.readStr(), Partition: r.u32(), DurableName: r.optionalStr(), Start: r.streamStart()}
	n := r.u32()
	s.Filter = make([]StreamFilter, 0, n)
	for i := uint32(0); i < n && r.err == nil; i++ {
		s.Filter = append(s.Filter, StreamFilter{Key: r.readStr(), Pattern: r.readStr()})
	}
	s.Prefetch = r.u32()
	s.AutoAck = r.readBool()
	return s, r.finish()
}

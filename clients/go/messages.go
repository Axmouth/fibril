package fibril

// This file maps the idiomatic message structs to and from the wire codec, in
// both directions for every op, so the field order lives in exactly one place.
// Byte layouts are byte-for-byte identical to the Rust reference and pinned by
// clients/wire_vectors.json. Option fields are Go pointers (nil = none).

// ResumeOutcome is the handshake outcome the broker reports.
type ResumeOutcome string

const (
	ResumeNew      ResumeOutcome = "new"
	ResumeResumed  ResumeOutcome = "resumed"
	ResumeNotFound ResumeOutcome = "resume_not_found"
	ResumeRejected ResumeOutcome = "resume_rejected"
)

var resumeOutcomeOrder = []ResumeOutcome{ResumeNew, ResumeResumed, ResumeNotFound, ResumeRejected}

func (w *writer) resumeOutcome(o ResumeOutcome) {
	for i, v := range resumeOutcomeOrder {
		if v == o {
			w.u8(uint8(i))
			return
		}
	}
	w.u8(0)
}

func (r *reader) resumeOutcome() ResumeOutcome {
	tag := r.u8()
	if int(tag) >= len(resumeOutcomeOrder) {
		if r.err == nil {
			r.err = &WireError{Kind: WireUnknownTag, Message: "wire: unknown resume outcome"}
		}
		return ResumeNew
	}
	return resumeOutcomeOrder[tag]
}

// ---- shared field groups ----------------------------------------------

func (w *writer) queueKey(topic string, partition uint32, group *string) {
	w.writeStr(topic)
	w.u32(partition)
	w.optionalStr(group)
}

func (r *reader) queueKey() (topic string, partition uint32, group *string) {
	return r.readStr(), r.u32(), r.optionalStr()
}

func (w *writer) settleTags(tags []DeliveryTag) {
	w.u32(uint32(len(tags)))
	for _, t := range tags {
		w.u64(t.Epoch)
	}
}

func (r *reader) settleTags() []DeliveryTag {
	n := r.u32()
	tags := make([]DeliveryTag, 0, n)
	for i := uint32(0); i < n && r.err == nil; i++ {
		tags = append(tags, DeliveryTag{Epoch: r.u64()})
	}
	return tags
}

// ---- handshake ---------------------------------------------------------

// ResumeIdentity is the identity the broker returns and the client offers on
// reconnect to resume a session.
type ResumeIdentity struct {
	OwnerID     UUID
	ClientID    UUID
	ResumeToken UUID
}

func (w *writer) optionalResumeIdentity(ri *ResumeIdentity) {
	if ri == nil {
		w.u8(0)
		return
	}
	w.u8(1)
	w.uuid(ri.OwnerID)
	w.uuid(ri.ClientID)
	w.uuid(ri.ResumeToken)
}

func (r *reader) optionalResumeIdentity() *ResumeIdentity {
	if r.u8() != 1 {
		return nil
	}
	return &ResumeIdentity{OwnerID: r.uuid(), ClientID: r.uuid(), ResumeToken: r.uuid()}
}

// Hello is the client's opening handshake frame.
type Hello struct {
	ClientName      string
	ClientVersion   string
	ProtocolVersion uint16
	Resume          *ResumeIdentity
}

func EncodeHello(h Hello) []byte {
	w := writer{}
	w.magic("FHL1")
	w.writeStr(h.ClientName)
	w.writeStr(h.ClientVersion)
	w.u16(h.ProtocolVersion)
	w.optionalResumeIdentity(h.Resume)
	return w.buf
}

func DecodeHello(body []byte) (Hello, error) {
	r := reader{buf: body}
	r.expectMagic("FHL1")
	h := Hello{
		ClientName:      r.readStr(),
		ClientVersion:   r.readStr(),
		ProtocolVersion: r.u16(),
		Resume:          r.optionalResumeIdentity(),
	}
	return h, r.finish()
}

// HelloOk is the broker's handshake reply.
type HelloOk struct {
	ProtocolVersion uint16
	OwnerID         UUID
	ClientID        UUID
	ResumeToken     UUID
	ResumeOutcome   ResumeOutcome
	ServerName      string
	Compliance      string
}

func EncodeHelloOk(h HelloOk) []byte {
	w := writer{}
	w.magic("FHO1")
	w.u16(h.ProtocolVersion)
	w.uuid(h.OwnerID)
	w.uuid(h.ClientID)
	w.uuid(h.ResumeToken)
	w.resumeOutcome(h.ResumeOutcome)
	w.writeStr(h.ServerName)
	w.writeStr(h.Compliance)
	return w.buf
}

func DecodeHelloOk(body []byte) (HelloOk, error) {
	r := reader{buf: body}
	r.expectMagic("FHO1")
	h := HelloOk{
		ProtocolVersion: r.u16(),
		OwnerID:         r.uuid(),
		ClientID:        r.uuid(),
		ResumeToken:     r.uuid(),
		ResumeOutcome:   r.resumeOutcome(),
		ServerName:      r.readStr(),
		Compliance:      r.readStr(),
	}
	return h, r.finish()
}

// Auth is a username/password authentication frame.
type Auth struct {
	Username string
	Password string
}

func EncodeAuth(a Auth) []byte {
	w := writer{}
	w.magic("FAU1")
	w.writeStr(a.Username)
	w.writeStr(a.Password)
	return w.buf
}

func DecodeAuth(body []byte) (Auth, error) {
	r := reader{buf: body}
	r.expectMagic("FAU1")
	a := Auth{Username: r.readStr(), Password: r.readStr()}
	return a, r.finish()
}

// ErrorMsg is a structured broker error.
type ErrorMsg struct {
	Code    uint16
	Message string
}

func EncodeError(e ErrorMsg) []byte {
	w := writer{}
	w.magic("FER1")
	w.u16(e.Code)
	w.writeStr(e.Message)
	return w.buf
}

func DecodeError(body []byte) (ErrorMsg, error) {
	r := reader{buf: body}
	r.expectMagic("FER1")
	e := ErrorMsg{Code: r.u16(), Message: r.readStr()}
	return e, r.finish()
}

// ---- publish -----------------------------------------------------------

// Publish is a publish request. Group, PartitionKey, and TTLms are optional
// (nil = absent).
type Publish struct {
	Topic               string
	Partition           uint32
	Group               *string
	RequireConfirm      bool
	ContentType         ContentType
	Headers             Headers
	Payload             []byte
	Published           uint64
	PartitionKey        []byte
	PartitioningVersion uint64
	TTLms               *uint64
}

func (w *writer) publishCommon(p Publish) {
	w.writeStr(p.Topic)
	w.optionalStr(p.Group)
	w.u32(p.Partition)
	w.writeBool(p.RequireConfirm)
	w.contentType(p.ContentType)
	w.headers(p.Headers)
	w.u64(p.Published)
	w.optionalBytes(p.PartitionKey)
	w.u64(p.PartitioningVersion)
	w.writeBytes(p.Payload)
}

func EncodePublish(p Publish) []byte {
	w := writer{}
	w.magic("FPB1")
	w.publishCommon(p)
	// Trailing so a peer that omits it still decodes (read as nil).
	w.optionalU64(p.TTLms)
	return w.buf
}

func DecodePublish(body []byte) (Publish, error) {
	r := reader{buf: body}
	r.expectMagic("FPB1")
	p := Publish{
		Topic:               r.readStr(),
		Group:               r.optionalStr(),
		Partition:           r.u32(),
		RequireConfirm:      r.readBool(),
		ContentType:         r.contentType(),
		Headers:             r.headers(),
		Published:           r.u64(),
		PartitionKey:        r.optionalBytes(),
		PartitioningVersion: r.u64(),
		Payload:             r.readBytes(),
	}
	// Trailing optional: absent when the peer has not been updated to send it.
	if r.remaining() > 0 {
		p.TTLms = r.optionalU64()
	}
	return p, r.finish()
}

// PublishDelayed is a publish scheduled to become visible at NotBefore.
type PublishDelayed struct {
	Topic               string
	Partition           uint32
	Group               *string
	RequireConfirm      bool
	NotBefore           uint64
	ContentType         ContentType
	Headers             Headers
	Payload             []byte
	Published           uint64
	PartitionKey        []byte
	PartitioningVersion uint64
}

func EncodePublishDelayed(p PublishDelayed) []byte {
	w := writer{}
	w.magic("FPD1")
	w.writeStr(p.Topic)
	w.optionalStr(p.Group)
	w.u32(p.Partition)
	w.writeBool(p.RequireConfirm)
	w.u64(p.NotBefore)
	w.contentType(p.ContentType)
	w.headers(p.Headers)
	w.u64(p.Published)
	w.optionalBytes(p.PartitionKey)
	w.u64(p.PartitioningVersion)
	w.writeBytes(p.Payload)
	return w.buf
}

func DecodePublishDelayed(body []byte) (PublishDelayed, error) {
	r := reader{buf: body}
	r.expectMagic("FPD1")
	p := PublishDelayed{
		Topic:               r.readStr(),
		Group:               r.optionalStr(),
		Partition:           r.u32(),
		RequireConfirm:      r.readBool(),
		NotBefore:           r.u64(),
		ContentType:         r.contentType(),
		Headers:             r.headers(),
		Published:           r.u64(),
		PartitionKey:        r.optionalBytes(),
		PartitioningVersion: r.u64(),
		Payload:             r.readBytes(),
	}
	return p, r.finish()
}

// PublishOk is the broker's confirmed-publish reply carrying the assigned offset.
type PublishOk struct {
	Offset uint64
}

func EncodePublishOk(o PublishOk) []byte {
	w := writer{}
	w.magic("FPO1")
	w.u64(o.Offset)
	return w.buf
}

func DecodePublishOk(body []byte) (PublishOk, error) {
	r := reader{buf: body}
	r.expectMagic("FPO1")
	o := PublishOk{Offset: r.u64()}
	return o, r.finish()
}

// ---- settle ------------------------------------------------------------

// DeliveryTag identifies a delivery to ack or nack.
type DeliveryTag struct {
	Epoch uint64
}

// Ack settles one or more deliveries as processed.
type Ack struct {
	Topic     string
	Group     *string
	Partition uint32
	Tags      []DeliveryTag
}

func EncodeAck(a Ack) []byte {
	w := writer{}
	w.magic("FAK1")
	w.writeStr(a.Topic)
	w.optionalStr(a.Group)
	w.u32(a.Partition)
	w.settleTags(a.Tags)
	return w.buf
}

func DecodeAck(body []byte) (Ack, error) {
	r := reader{buf: body}
	r.expectMagic("FAK1")
	a := Ack{Topic: r.readStr(), Group: r.optionalStr(), Partition: r.u32(), Tags: r.settleTags()}
	return a, r.finish()
}

// Nack returns deliveries unprocessed, optionally requeuing them (NotBefore
// delays the requeue).
type Nack struct {
	Topic     string
	Group     *string
	Partition uint32
	Tags      []DeliveryTag
	Requeue   bool
	NotBefore *uint64
}

func EncodeNack(n Nack) []byte {
	w := writer{}
	w.magic("FNK1")
	w.writeStr(n.Topic)
	w.optionalStr(n.Group)
	w.u32(n.Partition)
	w.settleTags(n.Tags)
	w.writeBool(n.Requeue)
	w.optionalU64(n.NotBefore)
	return w.buf
}

func DecodeNack(body []byte) (Nack, error) {
	r := reader{buf: body}
	r.expectMagic("FNK1")
	n := Nack{Topic: r.readStr(), Group: r.optionalStr(), Partition: r.u32(), Tags: r.settleTags()}
	n.Requeue = r.readBool()
	n.NotBefore = r.optionalU64()
	return n, r.finish()
}

// ---- subscribe ---------------------------------------------------------

// Subscribe requests delivery of one partition of a topic.
type Subscribe struct {
	Topic          string
	Partition      uint32
	Group          *string
	Prefetch       uint32
	AutoAck        bool
	ConsumerGroup  *string
	ConsumerTarget *uint32
	MemberID       *UUID
}

func EncodeSubscribe(s Subscribe) []byte {
	w := writer{}
	w.magic("FSB1")
	w.queueKey(s.Topic, s.Partition, s.Group)
	w.u32(s.Prefetch)
	w.writeBool(s.AutoAck)
	w.optionalStr(s.ConsumerGroup)
	w.optionalU32(s.ConsumerTarget)
	w.optionalUUID(s.MemberID)
	return w.buf
}

func DecodeSubscribe(body []byte) (Subscribe, error) {
	r := reader{buf: body}
	r.expectMagic("FSB1")
	s := Subscribe{}
	s.Topic, s.Partition, s.Group = r.queueKey()
	s.Prefetch = r.u32()
	s.AutoAck = r.readBool()
	s.ConsumerGroup = r.optionalStr()
	s.ConsumerTarget = r.optionalU32()
	s.MemberID = r.optionalUUID()
	return s, r.finish()
}

// SubscribeOk is the broker's subscribe reply, echoing the assignment and the
// server-chosen sub id.
type SubscribeOk struct {
	SubID          uint64
	Topic          string
	Partition      uint32
	Group          *string
	Prefetch       uint32
	ConsumerGroup  *string
	ConsumerTarget *uint32
	MemberID       *UUID
}

func EncodeSubscribeOk(s SubscribeOk) []byte {
	w := writer{}
	w.magic("FSO1")
	w.u64(s.SubID)
	w.queueKey(s.Topic, s.Partition, s.Group)
	w.u32(s.Prefetch)
	w.optionalStr(s.ConsumerGroup)
	w.optionalU32(s.ConsumerTarget)
	w.optionalUUID(s.MemberID)
	return w.buf
}

func DecodeSubscribeOk(body []byte) (SubscribeOk, error) {
	r := reader{buf: body}
	r.expectMagic("FSO1")
	s := SubscribeOk{SubID: r.u64()}
	s.Topic, s.Partition, s.Group = r.queueKey()
	s.Prefetch = r.u32()
	s.ConsumerGroup = r.optionalStr()
	s.ConsumerTarget = r.optionalU32()
	s.MemberID = r.optionalUUID()
	return s, r.finish()
}

// ---- deliver -----------------------------------------------------------

// Deliver is a broker-pushed message delivery.
type Deliver struct {
	SubID           uint64
	Topic           string
	Group           *string
	Partition       uint32
	Offset          uint64
	DeliveryTag     DeliveryTag
	Published       uint64
	PublishReceived uint64
	ContentType     ContentType
	Headers         Headers
	Payload         []byte
}

func EncodeDeliver(d Deliver) []byte {
	w := writer{}
	w.magic("FDL1")
	w.u64(d.SubID)
	w.writeStr(d.Topic)
	w.optionalStr(d.Group)
	w.u32(d.Partition)
	w.u64(d.Offset)
	w.u64(d.DeliveryTag.Epoch)
	w.u64(d.Published)
	w.u64(d.PublishReceived)
	w.contentType(d.ContentType)
	w.headers(d.Headers)
	w.writeBytes(d.Payload)
	return w.buf
}

func DecodeDeliver(body []byte) (Deliver, error) {
	r := reader{buf: body}
	r.expectMagic("FDL1")
	d := Deliver{SubID: r.u64(), Topic: r.readStr(), Group: r.optionalStr(), Partition: r.u32()}
	d.Offset = r.u64()
	d.DeliveryTag = DeliveryTag{Epoch: r.u64()}
	d.Published = r.u64()
	d.PublishReceived = r.u64()
	d.ContentType = r.contentType()
	d.Headers = r.headers()
	d.Payload = r.readBytes()
	return d, r.finish()
}

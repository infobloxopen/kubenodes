package kubenodes

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/miekg/dns"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/coredns/coredns/plugin/pkg/dnstest"
	"github.com/coredns/coredns/plugin/test"
	"github.com/coredns/coredns/request"
)

func TestServeDNSInternal(t *testing.T) {
	k := New([]string{"example.", "in-addr.arpa.", "ip6.arpa."})
	var internalCases = []test.Case{
		{
			Qname: "node1.example.", Qtype: dns.TypeA,
			Rcode: dns.RcodeSuccess,
			Answer: []dns.RR{
				test.A("node1.example.	5	IN	A	1.2.3.4"),
			},
		},
		{
			Qname: "node1.example.", Qtype: dns.TypeAAAA,
			Rcode: dns.RcodeSuccess,
			Answer: []dns.RR{
				test.AAAA("node1.example.	5	IN	AAAA	1:2:3::4"),
			},
		},
		{
			Qname: "node2.example.", Qtype: dns.TypeA,
			Rcode: dns.RcodeSuccess,
			Answer: []dns.RR{
				test.A("node2.example.	5	IN	A	1.2.3.5"),
				test.A("node2.example.	5	IN	A	1.2.3.6"),
			},
		},
		{
			Qname: "example.", Qtype: dns.TypeA,
			Rcode: dns.RcodeSuccess,
			Ns:    []dns.RR{k.soa()},
		},
		{
			Qname: "nonexistent-node.example.", Qtype: dns.TypeA,
			Rcode: dns.RcodeNameError,
			Ns:    []dns.RR{k.soa()},
		},
	}

	k.client = fake.NewSimpleClientset()
	ctx := context.Background()
	node1 := &core.Node{
		ObjectMeta: meta.ObjectMeta{
			Name: "node1",
		},
		Status: core.NodeStatus{
			Addresses: []core.NodeAddress{
				{Type: core.NodeInternalIP, Address: "1.2.3.4"},
				{Type: core.NodeInternalIP, Address: "1:2:3::4"},
				{Type: core.NodeExternalIP, Address: "5.6.7.8"},
			},
		},
	}
	node2 := &core.Node{
		ObjectMeta: meta.ObjectMeta{
			Name: "node2",
		},
		Status: core.NodeStatus{
			Addresses: []core.NodeAddress{
				{Type: core.NodeInternalIP, Address: "1.2.3.5"},
				{Type: core.NodeInternalIP, Address: "1.2.3.6"},
			},
		},
	}
	k.client.CoreV1().Nodes().Create(ctx, node1, meta.CreateOptions{})
	k.client.CoreV1().Nodes().Create(ctx, node2, meta.CreateOptions{})

	k.setWatch(ctx)
	go k.controller.Run(k.stopCh)
	defer close(k.stopCh)

	// quick and dirty wait for sync
	for !k.controller.HasSynced() {
		time.Sleep(100 * time.Millisecond)
	}

	runTests(t, ctx, k, internalCases)
}

func TestServeDNSExternal(t *testing.T) {
	k := New([]string{"example.", "in-addr.arpa.", "ip6.arpa."})
	k.ipType = core.NodeExternalIP
	k.dnsType = core.NodeExternalDNS

	var externalCases = []test.Case{
		{
			Qname: "node1.example.", Qtype: dns.TypeA,
			Rcode: dns.RcodeSuccess,
			Answer: []dns.RR{
				test.A("node1.example.	5	IN	A	5.6.7.8"),
			},
		},
		{
			Qname: "node1.example.", Qtype: dns.TypeAAAA,
			Rcode: dns.RcodeSuccess,
			Answer: []dns.RR{
				test.AAAA("node1.example.	5	IN	AAAA	5:6:7::8"),
			},
		},
		{
			Qname: "node2.example.", Qtype: dns.TypeA,
			Rcode: dns.RcodeSuccess,
			Answer: []dns.RR{
				test.A("node2.example.	5	IN	A	5.6.7.10"),
				test.A("node2.example.	5	IN	A	5.6.7.9"),
			},
		},
		{
			Qname: "example.", Qtype: dns.TypeA,
			Rcode: dns.RcodeSuccess,
			Ns:    []dns.RR{k.soa()},
		},
		{
			Qname: "nonexistent-node.example.", Qtype: dns.TypeA,
			Rcode: dns.RcodeNameError,
			Ns:    []dns.RR{k.soa()},
		},
	}

	k.client = fake.NewSimpleClientset()
	ctx := context.Background()
	node1 := &core.Node{
		ObjectMeta: meta.ObjectMeta{
			Name: "node1",
		},
		Status: core.NodeStatus{
			Addresses: []core.NodeAddress{
				{Type: core.NodeInternalIP, Address: "1.2.3.4"},
				{Type: core.NodeInternalIP, Address: "1:2:3::4"},
				{Type: core.NodeExternalIP, Address: "5.6.7.8"},
				{Type: core.NodeExternalIP, Address: "5:6:7::8"},
			},
		},
	}
	node2 := &core.Node{
		ObjectMeta: meta.ObjectMeta{
			Name: "node2",
		},
		Status: core.NodeStatus{
			Addresses: []core.NodeAddress{
				{Type: core.NodeExternalIP, Address: "5.6.7.9"},
				{Type: core.NodeExternalIP, Address: "5.6.7.10"},
			},
		},
	}
	k.client.CoreV1().Nodes().Create(ctx, node1, meta.CreateOptions{})
	k.client.CoreV1().Nodes().Create(ctx, node2, meta.CreateOptions{})

	k.setWatch(ctx)
	go k.controller.Run(k.stopCh)
	defer close(k.stopCh)

	// quick and dirty wait for sync
	for !k.controller.HasSynced() {
		time.Sleep(100 * time.Millisecond)
	}

	runTests(t, ctx, k, externalCases)
}

func TestServeDNSUpstream(t *testing.T) {
	k := New([]string{"example.", "in-addr.arpa.", "ip6.arpa."})
	k.Upstream = newTestUpstream("testup", net.ParseIP("4.3.2.1"))

	var externalCases = []test.Case{
		{
			Qname: "node1.example.", Qtype: dns.TypeA,
			Rcode: dns.RcodeSuccess,
			Answer: []dns.RR{
				test.A("node1.example.	5	IN	A	1.2.3.4"),
				test.A("node1.example.	5	IN	A	4.3.2.1"),
			},
		},
		{
			Qname: "node2.example.", Qtype: dns.TypeA,
			Rcode: dns.RcodeSuccess,
			Answer: []dns.RR{
				test.A("node2.example.	5	IN	A	1.2.3.4"),
			},
		},
	}

	k.client = fake.NewSimpleClientset()
	ctx := context.Background()
	node1 := &core.Node{
		ObjectMeta: meta.ObjectMeta{
			Name: "node1",
		},
		Status: core.NodeStatus{
			Addresses: []core.NodeAddress{
				{Type: core.NodeInternalIP, Address: "1.2.3.4"},
				{Type: core.NodeInternalDNS, Address: "testup"},
			},
		},
	}
	node2 := &core.Node{
		ObjectMeta: meta.ObjectMeta{
			Name: "node2",
		},
		Status: core.NodeStatus{
			Addresses: []core.NodeAddress{
				{Type: core.NodeInternalIP, Address: "1.2.3.4"},
				{Type: core.NodeInternalDNS, Address: "unresolvable"},
			},
		},
	}
	k.client.CoreV1().Nodes().Create(ctx, node1, meta.CreateOptions{})
	k.client.CoreV1().Nodes().Create(ctx, node2, meta.CreateOptions{})

	k.setWatch(ctx)
	go k.controller.Run(k.stopCh)
	defer close(k.stopCh)

	// quick and dirty wait for sync
	for !k.controller.HasSynced() {
		time.Sleep(100 * time.Millisecond)
	}

	runTests(t, ctx, k, externalCases)
}

func runTests(t *testing.T, ctx context.Context, k *KubeNodes, cases []test.Case) {
	for i, tc := range cases {
		r := tc.Msg()
		w := dnstest.NewRecorder(&test.ResponseWriter{})

		_, err := k.ServeDNS(ctx, w, r)
		if err != tc.Error {
			t.Errorf("Test %d: %v", i, err)
			return
		}

		if w.Msg == nil {
			t.Errorf("Test %d: nil message", i)
		}
		if err := test.SortAndCheck(w.Msg, tc); err != nil {
			t.Errorf("Test %d: %v", i, err)
		}
	}

}

type testUpstream struct {
	qname string
	resp  *dns.Msg
}

func newTestUpstream(qname string, ip net.IP) *testUpstream {
	return &testUpstream{qname, &dns.Msg{
		MsgHdr: dns.MsgHdr{Response: true},
		Answer: []dns.RR{&dns.A{Hdr: dns.RR_Header{Name: qname, Rrtype: dns.TypeA, Class: dns.ClassINET}, A: ip}},
	}}
}

func (tu *testUpstream) Lookup(ctx context.Context, r request.Request, name string, typ uint16) (*dns.Msg, error) {
	if tu.qname != name {
		return &dns.Msg{
			MsgHdr: dns.MsgHdr{Response: true, Rcode: dns.RcodeNameError},
		}, nil
	}
	return tu.resp, nil
}

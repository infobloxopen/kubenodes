package kubenodes

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"
	core "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/dnsutil"
	"github.com/coredns/coredns/plugin/pkg/fall"
	"github.com/coredns/coredns/plugin/pkg/upstream"
	"github.com/coredns/coredns/request"
)

// KubeNodes is a plugin that creates records for a Kubernetes cluster's nodes.
type KubeNodes struct {
	Next     plugin.Handler
	Zones    []string
	Upstream upstreamer

	Fall            fall.F
	ttl             uint32
	ipType, dnsType core.NodeAddressType

	// Kubernetes API interface
	client     kubernetes.Interface
	controller cache.Controller
	indexer    cache.Indexer

	// concurrency control to stop controller
	stopLock sync.Mutex
	shutdown bool
	stopCh   chan struct{}
}

type upstreamer interface {
	Lookup(ctx context.Context, state request.Request, name string, typ uint16) (*dns.Msg, error)
}

// New returns a initialized KubeNodes.
func New(zones []string) *KubeNodes {
	k := new(KubeNodes)
	k.Zones = zones
	k.Upstream = upstream.New()
	k.ttl = defaultTTL
	k.stopCh = make(chan struct{})
	k.ipType = core.NodeInternalIP
	k.dnsType = core.NodeInternalDNS
	return k
}

const (
	// defaultTTL to apply to all answers.
	defaultTTL = 5
)

// Name implements the Handler interface.
func (k KubeNodes) Name() string { return "kubenodes" }

// ServeDNS implements the plugin.Handler interface.
func (k KubeNodes) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: r}

	qname := state.Name()
	zone := plugin.Zones(k.Zones).Matches(qname)
	if zone == "" || !supportedQtype(state.QType()) {
		return plugin.NextOrFailure(k.Name(), k.Next, ctx, w, r)
	}
	zone = state.QName()[len(qname)-len(zone):] // maintain case of original query
	state.Zone = zone

	if len(zone) == len(qname) {
		writeResponse(w, r, nil, nil, []dns.RR{k.soa()}, dns.RcodeSuccess)
		return dns.RcodeSuccess, nil
	}

	// handle reverse lookups
	if state.QType() == dns.TypePTR {
		if addr := dnsutil.ExtractAddressFromReverse(qname); addr != "" {
			objs, err := k.indexer.ByIndex("reverse", addr)
			if err != nil {
				return dns.RcodeServerFailure, err
			}
			if len(objs) == 0 {
				if k.Fall.Through(state.Name()) {
					return plugin.NextOrFailure(k.Name(), k.Next, ctx, w, r)
				}
				writeResponse(w, r, nil, nil, []dns.RR{k.soa()}, dns.RcodeNameError)
				return dns.RcodeNameError, nil
			}
			var records []dns.RR
			for _, obj := range objs {
				node, ok := obj.(*core.Node)
				if !ok {
					return dns.RcodeServerFailure, fmt.Errorf("unexpected %q from *Node index", reflect.TypeOf(obj))
				}
				records = append(records, &dns.PTR{
					Hdr: dns.RR_Header{Name: qname, Rrtype: dns.TypePTR, Class: dns.ClassINET, Ttl: k.ttl},
					Ptr: dnsutil.Join(node.Name, k.Zones[0]),
				})
			}
			writeResponse(w, r, records, nil, nil, dns.RcodeSuccess)
			return dns.RcodeSuccess, nil
		}
	}

	nodeName := state.Name()[0 : len(qname)-len(zone)-1]
	if zone == "." {
		nodeName = state.Name()[0 : len(qname)-len(zone)]
	}

	// get the node by key name from the indexer
	item, exists, err := k.indexer.GetByKey(nodeName)
	if err != nil {
		return dns.RcodeServerFailure, err
	}

	if !exists {
		if k.Fall.Through(state.Name()) {
			return plugin.NextOrFailure(k.Name(), k.Next, ctx, w, r)
		}
		writeResponse(w, r, nil, nil, []dns.RR{k.soa()}, dns.RcodeNameError)
		return dns.RcodeNameError, nil
	}

	node, ok := item.(*core.Node)
	if !ok {
		return dns.RcodeServerFailure, fmt.Errorf("unexpected %q from *Node index", reflect.TypeOf(item))
	}

	// extract IPs from the node
	var ips []string
	for _, addr := range node.Status.Addresses {
		switch addr.Type {
		case k.ipType:
			ips = append(ips, addr.Address)
		case k.dnsType:
			// DNS type node addresses cant be represented as CNAMEs, since a CNAME record must be the only record with
			// it's name. So look up the IP address and append them to ips
			m, err := k.Upstream.Lookup(ctx, state, addr.Address, state.QType())
			if err != nil {
				return dns.RcodeServerFailure, err
			}
			for _, a := range m.Answer {
				switch a.Header().Rrtype {
				case dns.TypeA:
					ips = append(ips, a.(*dns.A).A.String())
				case dns.TypeAAAA:
					ips = append(ips, a.(*dns.AAAA).AAAA.String())
				}
			}
		}
	}

	// build response records
	var records []dns.RR
	if state.QType() == dns.TypeA {
		for _, ip := range ips {
			if strings.Contains(ip, ":") {
				continue
			}
			if netIP := net.ParseIP(ip); netIP != nil {
				records = append(records, &dns.A{A: netIP,
					Hdr: dns.RR_Header{Name: qname, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: k.ttl}})
			}
		}
	}
	if state.QType() == dns.TypeAAAA {
		for _, ip := range ips {
			if !strings.Contains(ip, ":") {
				continue
			}
			if netIP := net.ParseIP(ip); netIP != nil {
				records = append(records, &dns.AAAA{AAAA: netIP,
					Hdr: dns.RR_Header{Name: qname, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: k.ttl}})
			}
		}
	}

	writeResponse(w, r, records, nil, nil, dns.RcodeSuccess)
	return dns.RcodeSuccess, nil
}

func writeResponse(w dns.ResponseWriter, r *dns.Msg, answer, extra, ns []dns.RR, rcode int) {
	m := new(dns.Msg)
	m.SetReply(r)
	m.Rcode = rcode
	m.Authoritative = true
	m.Answer = answer
	m.Extra = extra
	m.Ns = ns
	w.WriteMsg(m)
}

func (k KubeNodes) soa() *dns.SOA {
	return &dns.SOA{
		Hdr:     dns.RR_Header{Name: k.Zones[0], Rrtype: dns.TypeSOA, Class: dns.ClassINET, Ttl: k.ttl},
		Ns:      dnsutil.Join("ns.dns", k.Zones[0]),
		Mbox:    dnsutil.Join("hostmaster.dns", k.Zones[0]),
		Serial:  uint32(time.Now().Unix()),
		Refresh: 7200,
		Retry:   1800,
		Expire:  86400,
		Minttl:  k.ttl,
	}
}

func supportedQtype(qtype uint16) bool {
	switch qtype {
	case dns.TypeA, dns.TypeAAAA, dns.TypePTR:
		return true
	default:
		return false
	}
}

// Ready implements the ready.Readiness interface.
func (k *KubeNodes) Ready() bool { return k.controller.HasSynced() }

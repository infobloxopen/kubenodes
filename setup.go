package kubenodes

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
	clog "github.com/coredns/coredns/plugin/pkg/log"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"
)

const pluginName = "kubenodes"

var log = clog.NewWithPlugin(pluginName)

func init() { plugin.Register(pluginName, setup) }

func setup(c *caddy.Controller) error {
	k, err := parse(c)
	if err != nil {
		return plugin.Error(pluginName, err)
	}

	onStart, onShut, err := k.InitAPIConn(context.Background())
	if err != nil {
		return plugin.Error(pluginName, err)
	}
	if onStart != nil {
		c.OnStartup(onStart)
	}
	if onShut != nil {
		c.OnShutdown(onShut)
	}

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		k.Next = next
		return k
	})

	return nil
}

func parse(c *caddy.Controller) (*KubeNodes, error) {
	var (
		kns *KubeNodes
		err error
	)

	i := 0
	for c.Next() {
		if i > 0 {
			return nil, plugin.ErrOnce
		}
		i++

		kns, err = parseStanza(c)
		if err != nil {
			return kns, err
		}
	}
	return kns, nil
}

// parseStanza parses a kubenodes stanza
func parseStanza(c *caddy.Controller) (*KubeNodes, error) {
	kns := New(plugin.OriginsFromArgsOrServerBlock(c.RemainingArgs(), c.ServerBlockKeys))

	for c.NextBlock() {
		switch c.Val() {
		case "endpoint":
			args := c.RemainingArgs()
				if len(args) != 1 {
					return nil, c.ArgErr()
				}
				kns.APIServer = args[0]
		case "tls": // cert key ca
			args := c.RemainingArgs()
			if len(args) == 3 {
				kns.APIClientCert, kns.APIClientKey, kns.APICertAuth = args[0], args[1], args[2]
				continue
			}
			return nil, c.ArgErr()
		case "fallthrough":
			kns.Fall.SetZonesFromArgs(c.RemainingArgs())
		case "ttl":
			args := c.RemainingArgs()
			if len(args) == 0 {
				return nil, c.ArgErr()
			}
			t, err := strconv.Atoi(args[0])
			if err != nil {
				return nil, err
			}
			if t < 0 || t > 3600 {
				return nil, c.Errf("ttl must be in range [0, 3600]: %d", t)
			}
			kns.ttl = uint32(t)
		case "kubeconfig":
			args := c.RemainingArgs()
			if len(args) != 1 && len(args) != 2 {
				return nil, c.ArgErr()
			}
			overrides := &clientcmd.ConfigOverrides{}
			if len(args) == 2 {
				overrides.CurrentContext = args[1]
			}
			config := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
				&clientcmd.ClientConfigLoadingRules{ExplicitPath: args[0]},
				overrides,
			)
			kns.ClientConfig = config
		default:
			return nil, c.Errf("unknown property '%s'", c.Val())
		}
	}

	return kns, nil
}

func (k *KubeNodes) getClientConfig() (*rest.Config, error) {
	if k.ClientConfig != nil {
		return k.ClientConfig.ClientConfig()
	}
	loadingRules := &clientcmd.ClientConfigLoadingRules{}
	overrides := &clientcmd.ConfigOverrides{}
	clusterinfo := api.Cluster{}
	authinfo := api.AuthInfo{}

	// Connect to API from in cluster
	if k.APIServer == "" {
		cc, err := rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
		cc.ContentType = "application/vnd.kubernetes.protobuf"
		return cc, err
	}

	// Connect to API from out of cluster
	clusterinfo.Server = k.APIServer

	if len(k.APICertAuth) > 0 {
		clusterinfo.CertificateAuthority = k.APICertAuth
	}
	if len(k.APIClientCert) > 0 {
		authinfo.ClientCertificate = k.APIClientCert
	}
	if len(k.APIClientKey) > 0 {
		authinfo.ClientKey = k.APIClientKey
	}

	overrides.ClusterInfo = clusterinfo
	overrides.AuthInfo = authinfo
	clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)

	cc, err := clientConfig.ClientConfig()
	if err != nil {
		return nil, err
	}
	cc.ContentType = "application/vnd.kubernetes.protobuf"
	return cc, err

}

// InitAPIConn initializes a new KubeNodes connection.
func (k *KubeNodes) InitAPIConn(ctx context.Context) (onStart func() error, onShut func() error, err error) {
	if k.client == nil {
		config, err := k.getClientConfig()
		if err != nil {
			return nil, nil, err
		}

		kubeClient, err := kubernetes.NewForConfig(config)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create kubernetes notification controller: %q", err)
		}
		k.client = kubeClient
	}
	k.indexer, k.controller = cache.NewIndexerInformer(
		&cache.ListWatch{
			ListFunc: func(o v1.ListOptions) (runtime.Object, error) {
				return k.client.CoreV1().Nodes().List(ctx, o)
			},
			WatchFunc: func(o v1.ListOptions) (watch.Interface, error) {
				return k.client.CoreV1().Nodes().Watch(ctx, o)
			},
		},
		&core.Node{},
		0,
		cache.ResourceEventHandlerFuncs{},
		cache.Indexers{"reverse": func (obj interface{}) ([]string, error) {
			node, ok := obj.(*core.Node)
			if !ok {
				return nil, errors.New("unexpected obj type")
			}
			var idx []string
			for _, addr := range node.Status.Addresses {
				if addr.Type != core.NodeInternalIP {
					continue
				}
				idx = append(idx, addr.Address)
			}
			return idx, nil
		}},
	)

	onStart = func() error {
		go k.controller.Run(k.stopCh)
		return nil
	}

	onShut = func() error {
		k.stopLock.Lock()
		defer k.stopLock.Unlock()
		if !k.shutdown {
			close(k.stopCh)
			k.shutdown = true
			return nil
		}
		return fmt.Errorf("shutdown already in progress")
	}

	return onStart, onShut, err
}


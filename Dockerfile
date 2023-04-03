FROM golang:1.18.10-bullseye as builder

ARG coredns_version=1.10.1
ARG plugin_name=kubenodes
ARG plugin_repo=github.com/infobloxopen/kubenodes

RUN go mod download github.com/coredns/coredns@v${coredns_version}

WORKDIR $GOPATH/pkg/mod/github.com/coredns/coredns@v${coredns_version}
RUN go mod download

RUN sed -i "/kubernetes/i ${plugin_name}:${plugin_repo}" plugin.cfg && \
    echo "kubeapi:github.com/coredns/kubeapi" >> plugin.cfg && \
    go get ${plugin_repo}

RUN make coredns && \
    mv coredns /tmp/coredns

FROM scratch

COPY --from=builder /etc/ssl/certs /etc/ssl/certs
COPY --from=builder /tmp/coredns /coredns

EXPOSE 53 53/udp
ENTRYPOINT ["/coredns"]

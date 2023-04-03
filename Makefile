GIT_SHA ?= $(shell echo `git rev-parse --verify HEAD^{commit}`)
GITHUB_REPOSITORY ?= example/kubenodes
IMAGE_NAME ?= ghcr.io/${GITHUB_REPOSITORY}-coredns
TEST_IMAGE = ${IMAGE_NAME}:${GIT_SHA}

default: build-image

test:
	go test

build-image:
	docker build -t ${TEST_IMAGE} .

push-image:
	docker push ${TEST_IMAGE}

pull-image:
	docker pull ${TEST_IMAGE}

test-image:
	docker run --rm ${TEST_IMAGE} -plugins | grep kubenodes

RELEASE_IMAGE = ${IMAGE_NAME}:$(subst refs/tags/,,${GIT_REF})
promote-image:
ifndef GIT_REF
	$(error GIT_REF is not set)
endif
	docker tag ${TEST_IMAGE} ${RELEASE_IMAGE}
	docker push ${RELEASE_IMAGE}

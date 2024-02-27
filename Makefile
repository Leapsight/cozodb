REBAR3 ?= $(shell test -e `which rebar3` 2>/dev/null && which rebar3 || echo "./rebar3")

COZODB_TMP_DIR ?= "/tmp/cozodb/"

.PHONY: all
all: build

.PHONY: build
build: $(REBAR3)
	@$(REBAR3) compile

.PHONY: deps
deps:
	@$(REBAR3) deps

.PHONY: shell
shell:

	@$(REBAR3) shell

.PHONY: clean
clean:
	@$(REBAR3) cargo clean
	@$(REBAR3) clean

.PHONY: clean-all
clean-all:
	rm -rf $(CURDIR)/priv/crates
	rm -rf $(CURDIR)/_build

.PHONY: distclean
distclean: clean
	@$(REBAR3) clean --all

.PHONY: docs
docs:
	@$(REBAR3) edoc

.PHONY: eunit
eunit:
	@$(REBAR3) eunit

.PHONY: ct
ct:
	@COZODB_TMP_DIR=$(COZODB_TMP_DIR) $(REBAR3) ct

.PHONY: xref
xref:
	@$(REBAR3) xref


.PHONY: cover
cover:
	@$(REBAR3) cover

.PHONY: proper
proper:
	@$(REBAR3) proper


.PHONY: dyalizer
dyalizer:
	@$(REBAR3) dyalizer

.PHONY: test
test: eunit ct

.PHONY: release
release: xref
	@$(REBAR3) as prod release
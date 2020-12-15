# You can set these variables from the command line.
POETRY        := $(HOME)/.poetry/bin/poetry
SPHINXBUILD   := $(POETRY) run sphinx-build
SPHINXOPTS    :=
PAPER         :=
BUILDDIR      := _build
SOURCEDIR     := source

# Internal variables.
PAPEROPT_a4     := -D latex_paper_size=a4
PAPEROPT_letter := -D latex_paper_size=letter
ALLSPHINXOPTS   := -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) $(SOURCEDIR)
# the i18n builder cannot share the environment and doctrees with the others
I18NSPHINXOPTS  := $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) $(SOURCEDIR)

.PHONY: all
all: dirhtml

.PHONY: pristine
pristine: clean
	git clean -dfX

.PHONY: setup
setup:
	./_utils/setup.sh
	./_utils/pre-build.sh

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)/*

.PHONY: preview
preview: setup
	$(POETRY) run sphinx-autobuild -b dirhtml $(ALLSPHINXOPTS) $(BUILDDIR)/dirhtml --port 5500

.PHONY: dirhtml
dirhtml: setup
	$(SPHINXBUILD) -b dirhtml $(ALLSPHINXOPTS) $(BUILDDIR)/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

.PHONY: singlehtml
singlehtml: setup
	$(SPHINXBUILD) -b singlehtml $(ALLSPHINXOPTS) $(BUILDDIR)/singlehtml
	@echo
	@echo "Build finished. The HTML page is in $(BUILDDIR)/singlehtml."

.PHONY: epub
epub: 	setup
	$(SPHINXBUILD) -b epub $(ALLSPHINXOPTS) $(BUILDDIR)/epub
	@echo
	@echo "Build finished. The epub file is in $(BUILDDIR)/epub."

.PHONY: epub3
epub3: setup
	$(SPHINXBUILD) -b epub3 $(ALLSPHINXOPTS) $(BUILDDIR)/epub3
	@echo
	@echo "Build finished. The epub3 file is in $(BUILDDIR)/epub3."

.PHONY: dummy
dummy: setup
	$(SPHINXBUILD) -b dummy $(ALLSPHINXOPTS) $(BUILDDIR)/dummy
	@echo
	@echo "Build finished. Dummy builder generates no files."

.PHONY: linkcheck
linkcheck: setup
	$(SPHINXBUILD) -b linkcheck $(SOURCEDIR) $(BUILDDIR)/linkcheck

GIT_BRANCH           = $(shell git branch --show-current)
GIT_BRANCH_FORMATTED = $(GIT_BRANCH:branch-%=%)

.PHONY: multiversion
multiversion: setup
	@mkdir -p $(HOME)/.cache/pypoetry/virtualenvs
	$(POETRY) run sphinx-multiversion --pre-build ./_utils/pre-build.sh \
	-D smv_branch_whitelist=$(GIT_BRANCH) -D smv_outputdir_format=$(GIT_BRANCH_FORMATTED) \
	$(SOURCEDIR) $(BUILDDIR)/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

# Deploy targets does not have dependency to multiversion to

GH_PAGES_DIR := $(HOME)/gh-pages

.PHONY: deploy
deploy:
	./_utils/checkout-gh-pages.sh $(GH_PAGES_DIR)
	rm -rf $(GH_PAGES_DIR)/$(GIT_BRANCH_FORMATTED)
	cp -r $(BUILDDIR)/dirhtml/$(GIT_BRANCH_FORMATTED) $(GH_PAGES_DIR)
	./_utils/commit-gh-pages.sh $(GH_PAGES_DIR) "Update $(GIT_BRANCH_FORMATTED)"

.PHONY: deploy-root
deploy-root:
	./_utils/checkout-gh-pages.sh $(GH_PAGES_DIR)
	rm -rf $(GH_PAGES_DIR)/_static
	cp -r $(BUILDDIR)/dirhtml/_static $(GH_PAGES_DIR)
	rm -rf $(GH_PAGES_DIR)/*.html
	cp $(BUILDDIR)/dirhtml/*.html $(GH_PAGES_DIR)
	./_utils/redirect.sh > $(GH_PAGES_DIR)/index.html
	./_utils/commit-gh-pages.sh $(GH_PAGES_DIR) "Update root"

.PHONY: help dev run build test test-coverage test-race lint vet fmt check \
       deps deps-update deps-clean clean clean-all \
       docker-build docker-run setup

include .env
export

# Configurações
APP_NAME=evo-bot-runtime
MAIN_PATH=./cmd/server
BUILD_DIR=bin
GO=go
GOFLAGS=-v

# Cores para output
GREEN=\033[0;32m
YELLOW=\033[0;33m
RED=\033[0;31m
NC=\033[0m # No Color

##@ Ajuda

help: ## Exibe esta mensagem de ajuda
	@echo "$(GREEN)Evo Bot Runtime - Makefile$(NC)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "\nUso:\n  make $(YELLOW)<target>$(NC)\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2 } /^##@/ { printf "\n$(YELLOW)%s$(NC)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Desenvolvimento

dev: ## Roda a aplicação em modo desenvolvimento
	@echo "$(GREEN)🔧 Iniciando Evo Bot Runtime em modo desenvolvimento...$(NC)"
	$(GO) run -race $(MAIN_PATH)

run: ## Roda a aplicação em modo produção
	@echo "$(GREEN)🚀 Iniciando Evo Bot Runtime...$(NC)"
	$(GO) run $(MAIN_PATH)

##@ Build

build: ## Compila a aplicação
	@echo "$(GREEN)🔨 Compilando $(APP_NAME)...$(NC)"
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(GOFLAGS) -o $(BUILD_DIR)/$(APP_NAME) $(MAIN_PATH)
	@echo "$(GREEN)✅ Build completo: $(BUILD_DIR)/$(APP_NAME)$(NC)"

build-linux: ## Compila para Linux
	@echo "$(GREEN)🔨 Compilando para Linux...$(NC)"
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=amd64 $(GO) build $(GOFLAGS) -o $(BUILD_DIR)/$(APP_NAME)-linux-amd64 $(MAIN_PATH)
	@echo "$(GREEN)✅ Build Linux completo$(NC)"

##@ Testes

test: ## Roda todos os testes
	@echo "$(GREEN)🧪 Rodando testes...$(NC)"
	$(GO) test -v ./...

test-coverage: ## Roda testes com cobertura
	@echo "$(GREEN)🧪 Rodando testes com cobertura...$(NC)"
	$(GO) test -v -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "$(GREEN)✅ Cobertura gerada: coverage.html$(NC)"

test-race: ## Roda testes verificando race conditions
	@echo "$(GREEN)🧪 Rodando testes com race detector...$(NC)"
	$(GO) test -race -v ./...

##@ Dependências

deps: ## Instala dependências
	@echo "$(GREEN)📦 Instalando dependências...$(NC)"
	$(GO) mod download
	$(GO) mod verify
	@echo "$(GREEN)✅ Dependências instaladas$(NC)"

deps-update: ## Atualiza dependências
	@echo "$(GREEN)📦 Atualizando dependências...$(NC)"
	$(GO) get -u ./...
	$(GO) mod tidy
	@echo "$(GREEN)✅ Dependências atualizadas$(NC)"

deps-clean: ## Limpa dependências não utilizadas
	@echo "$(GREEN)🧹 Limpando dependências...$(NC)"
	$(GO) mod tidy
	@echo "$(GREEN)✅ Dependências limpas$(NC)"

##@ Docker

docker-build: ## Build da imagem Docker
	@echo "$(GREEN)🐳 Construindo imagem Docker...$(NC)"
	docker build -t $(APP_NAME):latest .
	@echo "$(GREEN)✅ Imagem Docker construída$(NC)"

docker-run: ## Roda container Docker
	@echo "$(GREEN)🐳 Iniciando container...$(NC)"
	docker run -p 8080:8080 --env-file .env $(APP_NAME):latest

##@ Linting e Formatação

fmt: ## Formata o código
	@echo "$(GREEN)✨ Formatando código...$(NC)"
	$(GO) fmt ./...
	@echo "$(GREEN)✅ Código formatado$(NC)"

lint: ## Executa linter (requer golangci-lint)
	@echo "$(GREEN)🔍 Executando linter...$(NC)"
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint run ./...; \
		echo "$(GREEN)✅ Lint completo$(NC)"; \
	else \
		echo "$(RED)❌ golangci-lint não instalado. Instale com: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest$(NC)"; \
		exit 1; \
	fi

vet: ## Executa go vet
	@echo "$(GREEN)🔍 Executando go vet...$(NC)"
	$(GO) vet ./...
	@echo "$(GREEN)✅ Vet completo$(NC)"

check: fmt vet lint test ## Executa todas as verificações

##@ Limpeza

clean: ## Remove arquivos de build
	@echo "$(YELLOW)🧹 Limpando arquivos de build...$(NC)"
	@rm -rf $(BUILD_DIR)
	@rm -f coverage.out coverage.html
	@echo "$(GREEN)✅ Limpeza completa$(NC)"

clean-all: clean ## Remove arquivos de build e cache
	@echo "$(YELLOW)🧹 Limpeza completa (incluindo cache)...$(NC)"
	$(GO) clean -cache -testcache -modcache
	@echo "$(GREEN)✅ Limpeza completa$(NC)"

##@ Utilitários

setup: deps ## Setup completo do ambiente de desenvolvimento
	@echo "$(GREEN)🎉 Setup completo!$(NC)"
	@echo ""
	@echo "Para começar a desenvolver, rode:"
	@echo "  $(YELLOW)make dev$(NC)"
	@echo ""
	@echo "Outros comandos úteis:"
	@echo "  $(YELLOW)make help$(NC)       - Ver todos os comandos"
	@echo "  $(YELLOW)make test$(NC)       - Rodar testes"
	@echo "  $(YELLOW)make build$(NC)      - Compilar a aplicação"

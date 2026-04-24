Write-Host "=== SETUP DO AMBIENTE FATURAMENTO WEB ===" -ForegroundColor Cyan

# 1. Ir para a pasta onde o script está
$ProjetoPath = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjetoPath

Write-Host "Pasta do projeto: $ProjetoPath" -ForegroundColor Green

# 2. Procurar Python 3.12
Write-Host "`nVerificando Python 3.12..." -ForegroundColor Yellow

$pythonCmd = $null

try {
    py -3.12 --version | Out-Null
    $pythonCmd = "py -3.12"
    Write-Host "Python 3.12 encontrado via py launcher." -ForegroundColor Green
}
catch {
    Write-Host "Python 3.12 não encontrado pelo py launcher." -ForegroundColor Red
}

# 3. Se não achar, tenta python comum
if (-not $pythonCmd) {
    try {
        $versao = python --version
        if ($versao -like "*3.12*") {
            $pythonCmd = "python"
            Write-Host "Python 3.12 encontrado via python." -ForegroundColor Green
        }
    }
    catch {
        Write-Host "Python não encontrado no PATH." -ForegroundColor Red
    }
}

# 4. Se ainda não achou, para
if (-not $pythonCmd) {
    Write-Host "`nERRO: Python 3.12 não encontrado." -ForegroundColor Red
    Write-Host "Instale o Python 3.12 oficial e marque: Add to PATH, pip e venv."
    Write-Host "Depois rode este script novamente."
    exit 1
}

# 5. Remover venv antiga
if (Test-Path ".venv") {
    Write-Host "`nRemovendo .venv antiga..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force ".venv"
}

# 6. Criar nova venv
Write-Host "`nCriando nova .venv..." -ForegroundColor Yellow
Invoke-Expression "$pythonCmd -m venv .venv"

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    Write-Host "ERRO: Não foi possível criar a .venv." -ForegroundColor Red
    exit 1
}

# 7. Ativar venv
Write-Host "`nAtivando .venv..." -ForegroundColor Yellow
& ".\.venv\Scripts\Activate.ps1"

# 8. Validar Python da venv
Write-Host "`nVersão do Python na .venv:" -ForegroundColor Yellow
python --version

# 9. Atualizar pip
Write-Host "`nAtualizando pip, setuptools e wheel..." -ForegroundColor Yellow
python -m pip install --upgrade pip setuptools wheel

# 10. Instalar dependências base
if (Test-Path "requirements.txt") {
    Write-Host "`nInstalando requirements.txt..." -ForegroundColor Yellow
    pip install -r requirements.txt
}
else {
    Write-Host "`nrequirements.txt não encontrado. Instalando base manual..." -ForegroundColor Yellow
    pip install pandas sqlalchemy psycopg2-binary python-dotenv streamlit
}

# 11. Instalar dependências dos agentes
if (Test-Path "requirements-agentes.txt") {
    Write-Host "`nInstalando requirements-agentes.txt..." -ForegroundColor Yellow
    pip install -r requirements-agentes.txt
}
else {
    Write-Host "`nrequirements-agentes.txt não encontrado. Instalando agentes manual..." -ForegroundColor Yellow
    pip install crewai langchain-ollama
}

# 12. Validar principais pacotes
Write-Host "`nValidando pacotes..." -ForegroundColor Yellow

python -c "import pandas; print('pandas OK')"
python -c "import sqlalchemy; print('sqlalchemy OK')"
python -c "import dotenv; print('python-dotenv OK')"
python -c "import streamlit; print('streamlit OK')"

try {
    python -c "import crewai; print('crewai OK')"
}
catch {
    Write-Host "CrewAI não validou. Verifique erro acima." -ForegroundColor Red
}

try {
    python -c "import langchain_ollama; print('langchain-ollama OK')"
}
catch {
    Write-Host "langchain-ollama não validou. Verifique erro acima." -ForegroundColor Red
}

Write-Host "`n=== SETUP FINALIZADO ===" -ForegroundColor Cyan
Write-Host "Para ativar depois:" -ForegroundColor Green
Write-Host ".venv\Scripts\activate"
Write-Host "`nPara testar banco:"
Write-Host "python teste_banco.py"
Write-Host "`nPara rodar agentes:"
Write-Host "python crew_portal_faturamento.py"
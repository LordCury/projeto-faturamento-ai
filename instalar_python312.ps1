Write-Host "=== INSTALANDO PYTHON 3.12 ===" -ForegroundColor Cyan

# URL oficial do Python 3.12
$url = "https://www.python.org/ftp/python/3.12.7/python-3.12.7-amd64.exe"
$installer = "$env:TEMP\python312.exe"

Write-Host "Baixando Python 3.12..." -ForegroundColor Yellow
Invoke-WebRequest -Uri $url -OutFile $installer

Write-Host "Instalando Python 3.12..." -ForegroundColor Yellow

Start-Process $installer -Wait -ArgumentList `
    "/quiet InstallAllUsers=1 PrependPath=1 Include_pip=1 Include_test=0"

Write-Host "Instalação concluída!" -ForegroundColor Green

# Atualizar PATH da sessão
$env:Path += ";C:\Program Files\Python312;C:\Program Files\Python312\Scripts"

Write-Host "`nVerificando instalação..." -ForegroundColor Yellow

try {
    py -3.12 --version
    Write-Host "Python 3.12 instalado com sucesso!" -ForegroundColor Green
}
catch {
    Write-Host "Python instalado, mas reinicie o terminal para atualizar PATH." -ForegroundColor Yellow
}

Write-Host "`nAgora você pode rodar o setup novamente." -ForegroundColor Cyan
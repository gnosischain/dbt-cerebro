# Loads plain KEY=VALUE lines from .env into the process environment and
# forwards all arguments to the venv's dbt. Lets ad-hoc dbt commands run on
# Windows without manually exporting the ClickHouse credentials first.
#   .\scripts\run_dbt.ps1 compile --select foo
$repo = Split-Path -Parent $PSScriptRoot
Get-Content (Join-Path $repo ".env") | ForEach-Object {
    # Accept both `KEY=VALUE` and `$env:KEY = "VALUE"` styles (the .env file
    # keeps one block per shell dialect).
    if ($_ -match '^(?:\$env:)?([A-Z_][A-Z0-9_]*)\s*=\s*(.*)$') {
        $name = $Matches[1]
        $value = $Matches[2].Trim().Trim('"').Trim("'")
        [Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
}
& (Join-Path $repo ".venv\Scripts\dbt.exe") @args
exit $LASTEXITCODE

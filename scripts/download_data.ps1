# Fetch the NASA C-MAPSS dataset into data\raw\ (public mirror). Run from project root.
$dir = "data\raw"; New-Item -ItemType Directory -Force -Path $dir | Out-Null
$base = "https://raw.githubusercontent.com/edwardzjl/CMAPSSData/master"
foreach ($s in "FD001","FD002","FD003","FD004") {
  foreach ($p in "train","test","RUL") {
    Write-Host "downloading ${p}_${s}.txt"
    Invoke-WebRequest -Uri "$base/${p}_${s}.txt" -OutFile "$dir\${p}_${s}.txt"
  }
}
Write-Host "done -> $dir"

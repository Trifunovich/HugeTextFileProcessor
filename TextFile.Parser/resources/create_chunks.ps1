# Define the input file and output directories
$inputFile = "input_file_2024112517_10.txt"
$smallDir = "small_chunk_folder"
$mediumDir = "medium_chunk_folder"
$largeDir = "large_chunk_folder"

# Create a function to clean and create directories
function Initialize-Directory($dir) {
    if (Test-Path $dir) {
        Remove-Item -Recurse -Force $dir
    }
    New-Item -ItemType Directory -Path $dir -Force
}

# Create directories if they don't exist
New-Item -ItemType Directory -Path $smallDir -Force
New-Item -ItemType Directory -Path $mediumDir -Force
New-Item -ItemType Directory -Path $largeDir -Force

# Read only the first 1000000 lines from the input file
$lines = Get-Content $inputFile -TotalCount 1000000

# Helper function to split and write lines
function Write-SplitFiles($lines, $folder, $fileCount, $linesPerFile) {
    for ($i = 0; $i -lt $fileCount; $i++) {
        $start = $i * $linesPerFile
        $end = $start + $linesPerFile - 1
        $fileLines = $lines[$start..$end]
        $fileName = "${folder}/chunk_$($i + 1).txt"
        $fileLines | Set-Content $fileName
        Write-Host "File '$fileName' is done."
    }
}

# Split into small files (100 files with 10000 rows each)
Write-SplitFiles $lines $smallDir 100 10000

# Split into medium files (50 files with 20000 rows each)
Write-SplitFiles $lines $mediumDir 50 20000

# Split into large files (10 files with 100000 rows each)
Write-SplitFiles $lines $largeDir 10 100000

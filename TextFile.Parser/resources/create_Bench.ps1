# Define the input file and output files
$inputFile = "input_file_2024112517_10.txt"
$outputFile1 = "bench_100.txt"
$outputFile2 = "bench_10000.txt"
$outputFile3 = "bench_1000000.txt"

# Read only the first 1000000 lines from the input file
$lines = Get-Content $inputFile -TotalCount 1000000

# Write the first 100 lines to the first output file
$lines[0..99] | Set-Content $outputFile1
Write-Host "File 'bench_100.txt' is done."

# Write the first 10000 lines to the second output file
$lines[0..9999] | Set-Content $outputFile2
Write-Host "File 'bench_10000.txt' is done."

# Write the first 1000000 lines to the third output file
$lines[0..999999] | Set-Content $outputFile3
Write-Host "File 'bench_1000000.txt' is done."


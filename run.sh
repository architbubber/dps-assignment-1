#!/bin/bash

# List of Dropbox share links and filenames
declare -A files=(
  ["https://www.dropbox.com/sh/3ixxc9c7jm1rjrq/AACKrpX76iKMtyFX_oYbca7Aa?dl=0&preview=authors.csv"]="authors.csv"
  ["https://www.dropbox.com/sh/3ixxc9c7jm1rjrq/AACKrpX76iKMtyFX_oYbca7Aa?dl=0&preview=comments.csv"]="comments.csv"
  ["https://www.dropbox.com/sh/3ixxc9c7jm1rjrq/AACKrpX76iKMtyFX_oYbca7Aa?dl=0&preview=submissions.csv"]="submissions.csv"
  ["https://www.dropbox.com/sh/3ixxc9c7jm1rjrq/AACKrpX76iKMtyFX_oYbca7Aa?dl=0&preview=subreddits.csv"]="subreddits.csv"
)

# Create output directory if needed
mkdir -p downloaded_files

# Loop and download each file
for url in "${!files[@]}"; do
  filename="${files[$url]}"
  echo "Downloading $filename..."
  curl -L "${url/&dl=0/&raw=1}" -o "downloaded_files/$filename"
done

echo "✅ All files downloaded into 'downloaded_files/' directory."


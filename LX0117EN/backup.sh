#!/bin/bash

# This checks if the number of arguments is correct
# If the number of arguments is incorrect ( $# != 2) print error message and exit
#bash backup.sh /home/jvm/ /home/jvm/destDir
#bash backup.sh /home/jvm/TargetDir /home/jvm/destDir
#Target -> TargetDir
#Destination -> destDir

if [[ $# != 2 ]]
then
  echo "backup.sh  Target: $1 destination: $2"
  exit
fi

# This checks if argument 1 and argument 2 are valid directory paths
if [[ ! -d $1 ]] || [[ ! -d $2 ]]
then
  echo "Invalid directory path provided"
  exit
fi

# [TASK 1]
targetDirectory=$1
destinationDirectory=$2

# [TASK 2]
echo "The first argument is target Directory= $targetDirectory"
echo "The second argument is destination Directory= $destinationDirectory"

# [TASK 3]
currentTS=$(date +%s)

# [TASK 4]
backupFileName="backup-[$currentTS].tar.gz"

# We're going to:
  # 1: Go into the target directory
  # 2: Create the backup file
  # 3: Move the backup file to the destination directory

# To make things easier, we will define some useful variables...

# [TASK 5]
origAbsPath=$(pwd)

# [TASK 6]
cd $destinationDirectory # <-
destDirAbsPath=$(pwd)

# [TASK 7]
cd $origAbsPath # <-
cd $targetDirectory # <-

# [TASK 8]
yesterdayTS=$(($currentTS - (24 * 60 * 60)))

declare -a toBackup

for file in $(ls $(pwd)) # [TASK 9]
do
 file_last_modified_date=$(date -r $file +%s)
#echo "$file"
#echo $file_last_modified_date
  
 # [TASK 10]
  if [[ $file_last_modified_date > $yesterdayTS ]]
  then
    # [TASK 11]
    #echo "File/folder $file was modified in the last 24 hours"
    toBackup+=($file)
  fi
  
  #if [[ $file_last_modified_date < $yesterdayTS ]]
  #then
    # [TASK 11]
    #echo "File/folder $file was NOT modified in the last 24 hours"
  #fi

   
done

echo ${toBackup[@]}

# [TASK 12]
tar -czvf $backupFileName ${toBackup[@]}

# [TASK 13]

mv $backupFileName $destDirAbsPath
# Congratulations! You completed the final project for this course!

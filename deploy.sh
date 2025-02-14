#!/bin/bash
lastver=$(git describe --abbrev=0 --tags)
echo "Last tag was: $lastver"
echo "---"
read -e -p "New version: " ver
read -e -p "New dev version: " devver

sed -i -e "s/<version>.*<\/version>/<version>$ver<\/version>/g" README.md && \

git add -A && git commit -m "Release v$ver." && git push origin master && \
mvn --batch-mode -Dtag=${ver} release:prepare -DignoreSnapshots=true -Dresume=false -DreleaseVersion=${ver} -DdevelopmentVersion=${devver}-SNAPSHOT && \
mvn release:perform && \
echo "Maven release done, publishing release on GitHub..," && \
git log $lastver..HEAD --oneline > changelog.txt && \
echo "" >> changelog.txt && \
gh release create -F changelog.txt -t "v$ver" $ver && \
rm changelog.txt

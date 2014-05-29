#!/bin/sh
if [ $# -lt 1 ]; then
  echo "Error: Missing tag!"
  exit 1
fi

if [ $# -gt 1 ]; then
  echo "Error: Too many arguments!"
  exit 1
fi

if [ -n "$(git status --porcelain)" ]; then
  echo "Error: Outstanding changes are present, please commit them!"
  exit 1
fi

version=$1
topdir=`git rev-parse --show-toplevel`
cd $topdir/galaxy_wrappers/
sed -i "s|<requirement type=\"package\" version=\"[[:alnum:].]\{1,\}\">hadoop-galaxy</requirement>|<requirement type=\"package\" version=\"$version\">hadoop-galaxy</requirement>|" *.xml
sed -i -e "s|<package name=\"hadoop-galaxy\" version=\"[[:alnum:].]\{1,\}\">|<package name=\"hadoop-galaxy\" version=\"$version\">|" -e "s|<action type=\"shell_command\">git reset --hard [[:alnum:].]\{1,\}</action>|<action type=\"shell_command\">git reset --hard $version</action>|" tool_dependencies.xml
git commit -m "Prepare for release $version" .
git tag -a -m "Release $version ." $version
echo "Remember to push the new tag with:"
echo "$ git push --tags origin $version"

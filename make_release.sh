#!/bin/bash

set -o errexit
set -o nounset

PackageName="hadoop-galaxy"

function error() {
    if [ $# -ge 1 ]; then
        echo $* >&1
    fi
    exit 1
}

function usage_error() {
    error  "Usage: $0 RELEASE_NUM"
}

function validate_version_format() {
	local version_text="${1}"
	# accepts versions with three numbers and optional appendage
	# e.g., 1.0.0, 1.0.1-devel
	if ! python -c "import re, sys; sys.exit( 0 if re.match(r'\d+\.\d+\.\d+([-.].+)?$', '${version_text}') else 1)" ; then
			error "Please use a version name that of the form X.Y.Z"
	fi
	return 0
}

function confirm() {
	local prompt="${1}"
	echo "${prompt} [Y/n]"
	read -p "Answer: " yn
	case "${yn}" in
			''|[Yy]) # do nothing and keep going
					;;
			[Nn]) echo "Aborting"; exit 0
					;;
			*) usage_error "Unrecognized answer. Please specify Y or n"
					;;
	esac
	return 0
}

function update_tool_deps() {
	local next_version="${1}"
	printf -v sed_packge_expr  '/<package name="%s"/s/version="[^"]*"/version="%s"/' "${PackageName}" "${next_version}"
	printf -v sed_git_expr  '/git reset/s/git reset --hard [^<]\+/git reset --hard %s/' "${next_version}"

	sed -i -e "${sed_packge_expr}" -e "${sed_git_expr}" galaxy_wrappers/tool_dependencies.xml

	echo "Wrote version '${next_version}' to tool_dependencies.xml."
	return 0
}

function update_tools() {
	local next_version="${1}"
	printf -v sed_expr  '/<tool.*>/s/version="[^"]*"/version="%s"/' "${next_version}"
	sed -i -e "${sed_expr}" galaxy_wrappers/*.xml
	echo "Updated tool wrapper versions to ${next_version}"
	return 0
}

#### main ####

if [ $# -ne 1 ]; then
	usage_error
fi

next_version="${1}"

validate_version_format "${next_version}"

confirm "Are you sure you want to create the release named '${next_version}'?"

# ensure the tag doesn't already exist
if git tag -l | grep -w "${next_version}" ; then
    error "A release tag called '${next_version}' already exists"
fi

echo "Going forward with release '${next_version}'"

update_tool_deps "${next_version}"
update_tools "${next_version}"

git add galaxy_wrappers/*.xml
git commit -m "Release '${next_version}'"
git tag "${next_version}"

revid=$(git rev-parse HEAD)

echo "Tagged new commit ${revid} with tag '${next_version}'"
short_revid=${revid::8}

archive_name="hadoop_galaxy-${short_revid}.tar.gz"
echo "Making wrapper archive for the tool shed in ${archive_name}"

cd galaxy_wrappers
tar czf "../${archive_name}" --transform "s%^%hadoop_galaxy-${short_revid}/%" --exclude=tool_conf.xml *

echo "Done"
echo "Don't forget to push the new commit and tag and then upload the archive the toolshed!"

#!/bin/bash
# Read uv.lock and display information about dependencies:
#
# * Project dependencies (from pyproject.toml)
# * Dependencies of packages (from uv.lock)
# * Reverse dependencies (who requires package)
#
# Adapted from poetry-tools.sh for uv

helpstr="
   deps       [-lq] [<package>]
   deps  diff [<base-ref>=HEAD] [<target-ref>]

     -h, --help  -- display help

   deps  [-l]            -- show top-level project dependencies
          -l             -- include sub-dependencies (not fully implemented for project root yet)

   deps  [-q] <package>  -- show direct and reverse dependencies for the package
          -q             -- exit with status 0 if package is a dependency, 1 otherwise

   deps  diff [<base-ref>=HEAD]  [<target-ref>]
                         -- summarise version changes between two revisions
"

R=$'\033[0m'
B=$'\033[1m'
I=$'\033[3m'
RED=$'\033[1;38;5;204m'
GREEN=$'\033[1;38;5;150m'
YELLOW=$'\033[1;38;5;222m'
CYAN=$'\033[1;38;5;117m'
MAGENTA=$'\033[1;35m'
GREY=$'\033[1;38;5;243m'

lockfile=uv.lock
pyproject=pyproject.toml

msg() {
  printf >&2 '%s\n' " · $*"
}

error() {
  echo
  msg "${RED}ERROR:$R $*"$'\n'
  exit 1
}

# --- Parsing Logic ---

# Get dependencies of a specific package from uv.lock
_requires() {
  pkg=$1
  # awk script to find [[package]] block and extract dependencies names
  awk -v pkg="$pkg" '
    BEGIN { inside_pkg=0; inside_deps=0 }
    /^\[\[package\]\]/ { inside_pkg=0; inside_deps=0 }
    /^name = "/ { 
        match($0, /"([^"]+)"/, arr);
        if (arr[1] == pkg) inside_pkg=1;
    }
    inside_pkg && /^dependencies = \[/ { inside_deps=1; next }
    inside_deps && /^\]/ { inside_deps=0; inside_pkg=0 }
    inside_deps {
        if (match($0, /name = "([^"]+)"/, arr)) {
            print "    " "'"$B"'" arr[1] "'"$R"'" " requires"
        }
    }
  ' "$lockfile" | sort | uniq
}

# Get reverse dependencies (who depends on pkg) from uv.lock
_required_by() {
  pkg=$1
  echo "    $B$pkg$R is required by"
  awk -v pkg="$pkg" -v B="$B" -v R="$R" '
    BEGIN { current_pkg=""; inside_deps=0 }
    /^\[\[package\]\]/ { inside_deps=0 }
    /^name = "/ {
        match($0, /"([^"]+)"/, arr);
        current_pkg=arr[1];
    }
    /^dependencies = \[/ { inside_deps=1; next }
    /^\]/ { inside_deps=0 }
    inside_deps {
        if (match($0, /name = "([^"]+)"/, arr)) {
            if (arr[1] == pkg) {
                print "    " B current_pkg R
            }
        }
    }
  ' "$lockfile" | sort | uniq
}

# Get project top-level dependencies from pyproject.toml
_project_deps() {
  echo "$B PROJECT DEPENDENCIES $R"
  # Extract [project.dependencies]
  sed -n '/^\[project.dependencies\]/,/^\[/p' "$pyproject" | \
  grep -v '^\[' | grep -v '^$' | \
  sed 's/[" ,]//g' | \
  sed -E 's/([a-zA-Z0-9_-]+)(.*)/  \1 \2/' 

  echo
  echo "$B OPTIONAL/DEV DEPENDENCIES $R"
  # Extract [project.optional-dependencies] and [tool.hatch.envs.default] dependencies if exist
  # This is a bit rough, looking for common patterns
  sed -n '/^\[.*dependencies\]/,/^\[/p' "$pyproject" | \
  grep -v '\[project.dependencies\]' | \
  grep -v '^\[' | grep -v '^$' | \
  sed 's/[" ,]//g' | \
  sed -E 's/([a-zA-Z0-9_-]+)(.*)/  \1 \2/' 
}

# --- Diff Logic ---

deps_diff () {
  echo
  if (( ! $# )) && git diff-files --quiet $lockfile; then
    msg "$B$lockfile$R is no different from the committed or staged version"
    exit
  fi

  # Basic diff implementation for uv.lock
  # We look for lines changing 'version = "..."'
  
  git diff -U2 --word-diff=plain --word-diff-regex='[^. ]*' "$@" $lockfile |
    # Clean up output
    grep -v 'description = ' |
    grep '(^|[^{])version = .*(-\]|\+\})' -EC1 |
    sed -nr '
      /name = /{ s///; s/.*/'"$B&$R"'/; h; }
      /version = /{ 
         s///; 
         # Colorize removals
         s/\[-([^]]*)-\]/'"$RED\1$R"'/g
         # Colorize additions
         s/\{\+([^+]+)\+\}/'"$GREEN\1$R"'/g
         H; 
         x; 
         s/\n/ /g; 
         s/"//g;
         p; 
      }
    '
  echo
}

# --- Main Driver ---

show_help() {
  echo "$helpstr"
}

if [[ " $* " =~ \ (--help|-h|help)\  ]]; then
  show_help
  exit
fi

[[ -r $lockfile ]] || error $lockfile is not found in the working directory

if [[ $1 == diff ]]; then
  deps_diff "${@:2}"
  exit
fi

[[ " $* " == *" -q "* ]] && quiet=1
[[ " $* " == *" -l "* ]] && long=1
_pkg=(${@#-q})
_pkg=(${@#-l})
pkg=${_pkg[0]}

# Basic project info
project=$(grep '^name = ' $pyproject | head -1 | cut -d'"' -f2)
version=$(grep '^version = ' $pyproject | head -1 | cut -d'"' -f2)
# Try to find python version
python_req=$(grep '^requires-python = ' $pyproject | head -1 | cut -d'"' -f2)

if (( ! quiet )); then
  msg "Project: $B$project$R"
  # msg "Version: $B$version$R" # Version is often dynamic in hatch
  msg "Python:  $B$python_req$R"
fi

if (( ! ${#pkg} )); then
  _project_deps
else
  # Find package version in lockfile
  pkg_ver=$(awk -v pkg="$pkg" '$0 ~ "^name = \"" pkg "\"" { found=1; next } found && $0 ~ "^version =" { split($0,a,"\""); print a[2]; exit }' $lockfile)
  
  if [[ -z "$pkg_ver" ]]; then
     if (( quiet )); then exit 1; fi
     error "Package $B$pkg$R is not found in $lockfile"
  else
     if (( quiet )); then exit 0; fi
     msg "Package: $B$pkg$R $B$pkg_ver$R"
     echo
     _requires "$pkg"
     echo
     _required_by "$pkg"
     echo
  fi
fi

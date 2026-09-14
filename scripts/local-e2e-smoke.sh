#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
release_binary=${RIVET_E2E_BINARY:-$repo_root/target/release/rivet}

if [ ! -x "$release_binary" ]; then
    printf '%s\n' "local e2e smoke refused: release binary is missing: $release_binary" >&2
    exit 1
fi

e2e_data_dir=$(mktemp -d "${TMPDIR:-/tmp}/rivet-e2e.XXXXXX")
cleanup() {
    rm -rf -- "$e2e_data_dir"
}
trap cleanup EXIT HUP INT TERM

data_dir="$e2e_data_dir/.rivet"
project_name=rivet-e2e
clone_dir="$e2e_data_dir/cloned-repository"
remote_project_name=rivet-remote-e2e
remote_clone_dir="$e2e_data_dir/remote-project"

"$release_binary" scm clone "$repo_root" "$clone_dir" \
    --branch main --depth 1
test -f "$clone_dir/Rivetfile.toml"
test "$(git -C "$clone_dir" rev-parse --abbrev-ref HEAD)" = "main"

"$release_binary" --data-dir "$data_dir" project create "$remote_project_name" \
    --repository-url "$repo_root" --clone-destination "$remote_clone_dir" \
    --branch main --depth 1
test -f "$remote_clone_dir/Rivetfile.toml"
test "$(git -C "$remote_clone_dir" rev-parse --abbrev-ref HEAD)" = "main"

"$release_binary" --data-dir "$data_dir" project create "$project_name" --repository "$repo_root"
"$release_binary" --data-dir "$data_dir" run "$project_name"

builds_output=$("$release_binary" --data-dir "$data_dir" builds "$project_name")
printf '%s\n' "$builds_output"
printf '%s\n' "$builds_output" | rg -q '^#1[[:space:]]+passed[[:space:]]'

"$release_binary" --data-dir "$data_dir" inspect "$project_name" --build 1
"$release_binary" --data-dir "$data_dir" logs "$project_name" --build 1

# Exercise the complete local containerized-artifact path without installing
# or starting Docker/Podman. The shim preserves the runtime argument boundary,
# executes inside the mounted workspace, and lets the real server projector
# collect and expose the produced artifact.
container_runtime_dir="$e2e_data_dir/container-runtime"
container_repository="$e2e_data_dir/container-repository"
mkdir -p "$container_runtime_dir" "$container_repository"
cat > "$container_runtime_dir/podman" <<'SHIM'
#!/bin/sh
set -eu
workspace=
while [ "$#" -gt 0 ]; do
  case "$1" in
    run) shift ;;
    --volume)
      [ -n "$workspace" ] || workspace=${2%%:/rivet/workspace:rw}
      shift 2
      ;;
    --env|--network|--pull|--workdir) shift 2 ;;
    --rm|--init|--sig-proxy=true) shift ;;
    *)
      shift
      break
      ;;
  esac
done
test -n "$workspace"
cd "$workspace"
exec "$@"
SHIM
chmod 700 "$container_runtime_dir/podman"
cat > "$container_repository/Rivetfile.toml" <<'PIPELINE'
version = 1
name = "container-artifact"

[[artifacts]]
name = "container-bundle"
paths = ["dist/container.txt"]
stage = "Build"

[[stages]]
name = "Build"
[[stages.steps]]
name = "package"
program = "sh"
args = ["-c", "mkdir -p dist && printf 'container artifact\\n' > dist/container.txt"]
timeout_seconds = 30
[stages.steps.container]
runtime = "podman"
image = "fixture/runtime:1"
pull = "never"
network = "none"
PIPELINE
PATH="$container_runtime_dir:$PATH" "$release_binary" --data-dir "$data_dir" project create container-e2e --repository "$container_repository"
PATH="$container_runtime_dir:$PATH" "$release_binary" --data-dir "$data_dir" run container-e2e
container_builds=$("$release_binary" --data-dir "$data_dir" builds container-e2e)
printf '%s\n' "$container_builds"
printf '%s\n' "$container_builds" | rg -q '^#1[[:space:]]+passed[[:space:]]'
container_artifacts=$("$release_binary" --data-dir "$data_dir" artifacts container-e2e --build 1)
printf '%s\n' "$container_artifacts"
printf '%s\n' "$container_artifacts" | rg -q '^container-bundle[[:space:]]+dist/container\.txt[[:space:]]'

printf '%s\n' "local end-to-end smoke passed"

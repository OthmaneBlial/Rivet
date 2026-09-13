export type ExtensionKind = "wasm" | "subprocess";

export type ExtensionPermission =
  | "read_builds"
  | "read_logs"
  | "read_artifacts"
  | "trigger_builds"
  | "write_annotations";

export interface ExtensionManifest {
  protocol_version: number;
  id: string;
  name: string;
  version: string;
  kind: ExtensionKind;
  entrypoint: string;
  permissions: ExtensionPermission[];
}

export interface ExtensionCatalogModel {
  protocol_name: string;
  protocol_version: number;
  supported_kinds: ExtensionKind[];
  loaded: ExtensionManifest[];
}

export const EXTENSION_CATALOG: ExtensionCatalogModel = {
  protocol_name: "rivet-extension",
  protocol_version: 1,
  supported_kinds: ["wasm", "subprocess"],
  loaded: [],
};

export const EXTENSION_PERMISSION_LABELS: Record<ExtensionPermission, string> = {
  read_builds: "Read build state",
  read_logs: "Read signal stream",
  read_artifacts: "Read artifacts",
  trigger_builds: "Trigger builds",
  write_annotations: "Write annotations",
};

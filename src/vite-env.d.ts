/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_DEMO_MODE: string;
  readonly VITE_API_URL: string;
  readonly VITE_INTERNAL_API_KEY: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

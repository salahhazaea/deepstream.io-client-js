import eslint from '@eslint/js'
import tseslint from 'typescript-eslint'
import eslintConfigPrettier from 'eslint-config-prettier'
import eslintPluginImport from 'eslint-plugin-import'
import eslintPluginN from 'eslint-plugin-n'
import eslintPluginPromise from 'eslint-plugin-promise'
import typescriptEslintParser from '@typescript-eslint/parser'
import globals from 'globals'

const reactFiles = [
  'app/src/renderer/**/*{js,ts,tsx}',
  'hub/src/client/**/*.{js,ts,tsx}',
  'hub/.storybook/**/*.{js,ts,tsx}',
  'plugins/adobe/**/*{js,ts,tsx}',
]

export default tseslint.config(
  eslint.configs.recommended,
  ...tseslint.configs.recommended,

  {
    ignores: [
      'tmp*',
      'benchmarks/**/*',
    ],
  },

  // Base config:
  {
    plugins: {
      import: eslintPluginImport,
      promise: eslintPluginPromise,
    },
    languageOptions: {
      parser: typescriptEslintParser,
      parserOptions: {
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      'prefer-const': [
        'error',
        {
          destructuring: 'all',
        },
      ],
      'no-empty': ['error', { allowEmptyCatch: true }],
      'no-constant-condition': ['error', { checkLoops: false }],
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          args: 'none',
          ignoreRestSiblings: true,
        },
      ],
      'getter-return': 'off',
      'object-shorthand': ['warn', 'properties'],
    },
  },

  // Node specific
  {
    ignores: [...reactFiles],
    languageOptions: {
      globals: {
        ...globals.node,
      },
    },
    plugins: {
      n: eslintPluginN,
    },
    rules: {
      'no-redeclare': 'warn',
      'require-yield': 'off',
    },
  },

  // Prettier:
  eslintConfigPrettier
)

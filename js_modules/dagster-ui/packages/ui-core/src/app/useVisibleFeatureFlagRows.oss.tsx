import {FeatureFlag} from 'shared/app/FeatureFlags.oss';

/**
 * Open-source feature flags to be displayed in Dagster UI "User settings"
 */
export const useVisibleFeatureFlagRows = () => [
  {
    key: 'Display resources in navigation sidebar',
    flagType: FeatureFlag.flagSidebarResources,
  },
  {
    key: 'Disable WebSockets',
    flagType: FeatureFlag.flagDisableWebsockets,
  },
  {
    key: 'Disable automatically loading default config in launchpad',
    flagType: FeatureFlag.flagDisableAutoLoadDefaults,
  },
  {
    key: 'Debug console logging',
    flagType: FeatureFlag.flagDebugConsoleLogging,
  },
  {
    key: 'New navigation',
    label: (
      <>
        Experimental navigation (
        <a
          href="https://github.com/dagster-io/dagster/discussions/21370"
          target="_blank"
          rel="noreferrer"
        >
          Learn more
        </a>
        )
      </>
    ),
    flagType: FeatureFlag.flagSettingsPage,
  },
  {
    key: 'New code location page',
    flagType: FeatureFlag.flagCodeLocationPage,
  },
];

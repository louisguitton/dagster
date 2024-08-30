import {
  Box,
  Colors,
  FontFamily,
  MiddleTruncate,
  Mono,
  StyledRawCodeMirror,
  Table,
} from '@dagster-io/ui-components';
import {useContext, useMemo} from 'react';
import {CodeLocationServerSection} from 'shared/code-location/CodeLocationServerSection.oss';
import {CodeLocationTabs} from 'shared/code-location/CodeLocationTabs.oss';
import {createGlobalStyle} from 'styled-components';
import * as yaml from 'yaml';

import {CodeLocationOverviewSectionHeader} from './CodeLocationOverviewSectionHeader';
import {CodeLocationPageHeader} from './CodeLocationPageHeader';
import {TimeFromNow} from '../ui/TimeFromNow';
import {LocationStatus} from '../workspace/CodeLocationRowSet';
import {WorkspaceContext, WorkspaceRepositoryLocationNode} from '../workspace/WorkspaceContext';
import {RepoAddress} from '../workspace/types';
import {LocationStatusEntryFragment} from '../workspace/types/WorkspaceQueries.types';

const RIGHT_COLUMN_WIDTH = '280px';

type MetadataRowKey = 'image';

interface Props {
  repoAddress: RepoAddress;
  locationEntry: WorkspaceRepositoryLocationNode;
  locationStatus: LocationStatusEntryFragment;
}

export const CodeLocationOverviewRoot = (props: Props) => {
  const {repoAddress, locationStatus, locationEntry} = props;

  const {displayMetadata} = locationEntry;
  const metadataForDetails: Record<MetadataRowKey, {key: string; value: string} | null> =
    useMemo(() => {
      return {
        image: displayMetadata.find(({key}) => key === 'image') || null,
      };
    }, [displayMetadata]);

  const metadataAsYaml = useMemo(() => {
    return yaml.stringify(Object.fromEntries(displayMetadata.map(({key, value}) => [key, value])));
  }, [displayMetadata]);

  const libraryVersions = useMemo(() => {
    return locationEntry.locationOrLoadError?.__typename === 'RepositoryLocation'
      ? locationEntry.locationOrLoadError.dagsterLibraryVersions
      : null;
  }, [locationEntry]);

  return (
    <>
      <CodeLocationPageHeader repoAddress={repoAddress} />
      <Box padding={{horizontal: 24}} border="bottom">
        <CodeLocationTabs selectedTab="overview" repoAddress={repoAddress} />
      </Box>
      <CodeLocationOverviewSectionHeader label="Details" />
      {/* Fixed table layout to contain overflowing strings in right column */}
      <Table style={{width: '100%', tableLayout: 'fixed'}}>
        <tbody>
          <tr>
            <td
              style={{
                width: RIGHT_COLUMN_WIDTH,
                minWidth: RIGHT_COLUMN_WIDTH,
                verticalAlign: 'middle',
              }}
            >
              Status
            </td>
            <td>
              <LocationStatus locationStatus={locationStatus} locationOrError={locationEntry} />
            </td>
          </tr>
          <tr>
            <td>Updated</td>
            <td>
              <div style={{whiteSpace: 'nowrap'}}>
                <TimeFromNow unixTimestamp={locationStatus.updateTimestamp} />
              </div>
            </td>
          </tr>
          {metadataForDetails.image ? (
            <tr>
              <td>Image</td>
              <td style={{fontFamily: FontFamily.monospace}}>
                <MiddleTruncate text={metadataForDetails.image.value} />
              </td>
            </tr>
          ) : null}
        </tbody>
      </Table>
      <CodeLocationServerSection />
      {libraryVersions?.length ? (
        <>
          <CodeLocationOverviewSectionHeader label="Libraries" />
          <Table>
            <tbody>
              {libraryVersions.map((version) => (
                <tr key={version.name}>
                  <td style={{width: RIGHT_COLUMN_WIDTH}}>
                    <Mono>{version.name}</Mono>
                  </td>
                  <td>
                    <Mono>{version.version}</Mono>
                  </td>
                </tr>
              ))}
            </tbody>
          </Table>
        </>
      ) : null}
      <CodeLocationOverviewSectionHeader label="Metadata" border="bottom" />
      <CodeLocationMetadataStyle />
      <div style={{height: '320px'}}>
        <StyledRawCodeMirror
          options={{readOnly: true, lineNumbers: false}}
          theme={['code-location-metadata']}
          value={metadataAsYaml}
        />
      </div>
    </>
  );
};

const QueryfulCodeLocationOverviewRoot = ({repoAddress}: {repoAddress: RepoAddress}) => {
  const {locationEntries, locationStatuses, loading} = useContext(WorkspaceContext);
  const locationEntry = locationEntries.find((entry) => entry.name === repoAddress.location);
  const locationStatus = locationStatuses[repoAddress.location];
  if (!locationEntry || !locationStatus) {
    if (loading) {
      return <div />;
    }
    return <div>Oh no!</div>;
  }
  return (
    <CodeLocationOverviewRoot
      repoAddress={repoAddress}
      locationEntry={locationEntry}
      locationStatus={locationStatus}
    />
  );
};

// eslint-disable-next-line import/no-default-export
export default QueryfulCodeLocationOverviewRoot;

const CodeLocationMetadataStyle = createGlobalStyle`
  .CodeMirror.cm-s-code-location-metadata.cm-s-code-location-metadata {
    background-color: ${Colors.backgroundDefault()};
    padding: 12px 20px;
    height: 300px;
  }
`;

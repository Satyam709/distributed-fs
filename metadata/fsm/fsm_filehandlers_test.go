package fsm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// File Commands

func TestHandleCmdCreateFile(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandCreateFile
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:  "create new file with chunks",
			setup: func(m *MetadataFSM) {},
			req: CommandCreateFile{
				FileID:    "file-1",
				FileName:  "photos.tar",
				ChunkIDs:  []string{"ck-1", "ck-2"},
				FileSize:  2048,
				Checksum:  []byte{0xAB, 0xCD},
				CreatedAt: now,
			},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, err := m.GetFile("file-1")
				require.NoError(t, err)
				assert.Equal(t, "photos.tar", f.Filename)
				assert.Equal(t, FileStatusCreating, f.Status)
				assert.Equal(t, uint64(2048), f.FileSize)
				assert.Len(t, f.ChunkIDs, 2)

				for i, cid := range []string{"ck-1", "ck-2"} {
					c, err := m.GetChunk(cid)
					require.NoError(t, err)
					assert.Equal(t, "file-1", c.FileID)
					assert.Equal(t, i, c.ChunkIndex)
					assert.Equal(t, ChunkStatusAllocated, c.Status)
				}
			},
		},
		{
			name: "reject duplicate file ID",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "file-1", "old.txt", []string{"old-ck-1"})
			},
			req: CommandCreateFile{
				FileID:   "file-1",
				FileName: "new.txt",
				ChunkIDs: []string{"ck-new"},
				FileSize: 100,
			},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				// original file must be untouched
				f, _ := m.GetFile("file-1")
				assert.Equal(t, "old.txt", f.Filename)
			},
		},
		{
			name: "reject when chunk ID collides with existing registry",
			setup: func(m *MetadataFSM) {
				// pre-populate a chunk that conflicts
				m.chunkRegistry["ck-dup"] = &ChunkRecord{ChunkID: "ck-dup"}
			},
			req: CommandCreateFile{
				FileID:   "file-2",
				FileName: "test.bin",
				ChunkIDs: []string{"ck-dup"},
				FileSize: 50,
			},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				_, err := m.GetFile("file-2")
				assert.ErrorIs(t, err, ErrFileNotFound, "file must not be created on chunk collision")
			},
		},
		{
			name:  "create file with zero chunks",
			setup: func(m *MetadataFSM) {},
			req: CommandCreateFile{
				FileID:    "file-empty",
				FileName:  "empty.txt",
				ChunkIDs:  []string{},
				FileSize:  0,
				CreatedAt: now,
			},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, err := m.GetFile("file-empty")
				require.NoError(t, err)
				assert.Len(t, f.ChunkIDs, 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdCreateFile(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

func TestHandleCmdCommitFile(t *testing.T) {
	checksum := []byte{0x01, 0x02}

	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandCommitFile
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:       "file does not exist",
			setup:      func(m *MetadataFSM) {},
			req:        CommandCommitFile{FileID: "ghost", FileSize: 100, Checksum: checksum},
			wantErr:    true,
			assertPost: func(t *testing.T, m *MetadataFSM) {},
		},
		{
			name: "filesize mismatch",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "file-1", "f.bin", []string{})
			},
			req:     CommandCommitFile{FileID: "file-1", FileSize: 999, Checksum: checksum},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, _ := m.GetFile("file-1")
				assert.Equal(t, FileStatusCreating, f.Status, "status must not change on error")
			},
		},
		{
			name: "chunk not in complete status",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "file-1", "f.bin", []string{"ck-1"})
				// chunk is still in Allocated status
			},
			req:     CommandCommitFile{FileID: "file-1", FileSize: 100, Checksum: checksum},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, _ := m.GetFile("file-1")
				assert.Equal(t, FileStatusCreating, f.Status)
			},
		},
		{
			name: "successful commit stores checksum",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "file-1", "f.bin", []string{"ck-1", "ck-2"})
				m.chunkRegistry["ck-1"].Status = ChunkStatusComplete
				m.chunkRegistry["ck-2"].Status = ChunkStatusComplete
			},
			req:     CommandCommitFile{FileID: "file-1", FileSize: 100, Checksum: checksum},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, _ := m.GetFile("file-1")
				assert.Equal(t, FileStatusComplete, f.Status)
				assert.Equal(t, checksum, f.CheckSum,
					"CommitFile must store the provided checksum in FileRecord")
			},
		},
		{
			name: "commit with nil checksum still transitions status",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "file-1", "f.bin", []string{"ck-1"})
				m.chunkRegistry["ck-1"].Status = ChunkStatusComplete
			},
			req:     CommandCommitFile{FileID: "file-1", FileSize: 100, Checksum: nil},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, _ := m.GetFile("file-1")
				assert.Equal(t, FileStatusComplete, f.Status)
				assert.Nil(t, f.CheckSum,
					"nil checksum should be stored as nil")
			},
		},
		{
			name: "commit file with zero chunks",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "file-e", "empty.bin", []string{})
				m.fileIndex["file-e"].FileSize = 0
			},
			req:     CommandCommitFile{FileID: "file-e", FileSize: 0, Checksum: checksum},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, _ := m.GetFile("file-e")
				assert.Equal(t, FileStatusComplete, f.Status)
				assert.Equal(t, checksum, f.CheckSum)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdCommitFile(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

func TestHandleCmdDeleteFile(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandDeleteFile
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:       "delete non-existent file",
			setup:      func(m *MetadataFSM) {},
			req:        CommandDeleteFile{FileID: "ghost"},
			wantErr:    true,
			assertPost: func(t *testing.T, m *MetadataFSM) {},
		},
		{
			name: "soft-delete existing file",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "file-1", "doomed.txt", []string{"ck-1"})
			},
			req:     CommandDeleteFile{FileID: "file-1"},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, _ := m.GetFile("file-1")
				assert.Equal(t, FileStatusDeleted, f.Status)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdDeleteFile(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

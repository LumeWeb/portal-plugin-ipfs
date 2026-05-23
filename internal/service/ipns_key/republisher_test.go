package ipns_key

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/namesys"
	"github.com/ipfs/boxo/path"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.uber.org/zap/zaptest"
)

type mockPublisher struct {
	publishFn func(ctx context.Context, sk ic.PrivKey, value path.Path, options ...namesys.PublishOption) error
	calls     []publishCall
	mu        sync.Mutex
}

type publishCall struct {
	sk      ic.PrivKey
	value   path.Path
	options []namesys.PublishOption
}

func (m *mockPublisher) Publish(ctx context.Context, sk ic.PrivKey, value path.Path, options ...namesys.PublishOption) error {
	m.mu.Lock()
	m.calls = append(m.calls, publishCall{sk: sk, value: value, options: options})
	m.mu.Unlock()
	if m.publishFn != nil {
		return m.publishFn(ctx, sk, value, options...)
	}
	return nil
}

func (m *mockPublisher) getCalls() []publishCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

var republisherTestOptions = coreTesting.CombineOptions(
	coreTesting.WithServiceFactory(pluginCore.IPNS_KEY_SERVICE, NewIPNSKeyService),
	coreTesting.WithMockServiceFactory(pluginCore.FILE_MANAGER_SERVICE, mocks.NewMockFileManagerService),
	util.GetProtocolMock(),
	coreTesting.WithProtocolConfig("ipfs", &pluginConfig.ProtocolConfig{}),
	coreTesting.WithSQLitePluginMigrations(
		"ipfs", migrations.GetSQLite(),
	),
)

func TestRepublisher_RepublishAll(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		svc := keyService.(*IPNSKeyServiceDefault)

		pub := &mockPublisher{}
		dbKs := NewDBKeystore(svc.DB(), svc.decryptPrivateKey, zaptest.NewLogger(t))
		selfKey, _, _ := ic.GenerateEd25519Key(nil)

		r := NewRepublisher(pub, dbKs, selfKey, svc.decryptPrivateKey, zaptest.NewLogger(t))

		err := r.republishAll(context.Background())
		assert.NoError(tb, err)
		assert.Empty(tb, pub.getCalls(), "No keys with CID → nothing to republish")

		key, err := keyService.CreateKey(context.Background(), 1, "republish-key", KeyType_Ed25519)
		require.NoError(tb, err)
		_ = key

		err = r.republishAll(context.Background())
		assert.NoError(tb, err)
		assert.Empty(tb, pub.getCalls(), "Key without LastPublishedCID → nothing to republish")
	}, republisherTestOptions)
}

func TestRepublisher_SetInterval(t *testing.T) {
	r := &Republisher{interval: defaultRepublishInterval}
	assert.Equal(t, defaultRepublishInterval, r.interval)

	r.SetInterval(5 * time.Minute)
	assert.Equal(t, 5*time.Minute, r.interval)
}

func TestRepublisher_SetRecordLifetime(t *testing.T) {
	r := &Republisher{recordLifetime: defaultRecordLifetime}
	assert.Equal(t, defaultRecordLifetime, r.recordLifetime)

	r.SetRecordLifetime(24 * time.Hour)
	assert.Equal(t, 24*time.Hour, r.recordLifetime)
}

func TestRepublisher_Run_CancelsOnStop(t *testing.T) {
	pub := &mockPublisher{}
	r := &Republisher{
		publisher:      pub,
		interval:       1 * time.Hour,
		recordLifetime: defaultRecordLifetime,
		log:            zaptest.NewLogger(t),
	}

	stop := r.Run()
	time.Sleep(100 * time.Millisecond)
	stop()

	assert.Empty(t, pub.getCalls(), "Initial delay should prevent immediate republish")
}

func TestRepublisher_PublishError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		svc := keyService.(*IPNSKeyServiceDefault)

		pub := &mockPublisher{
			publishFn: func(_ context.Context, _ ic.PrivKey, _ path.Path, _ ...namesys.PublishOption) error {
				return assert.AnError
			},
		}
		dbKs := NewDBKeystore(svc.DB(), svc.decryptPrivateKey, zaptest.NewLogger(t))
		selfKey, _, _ := ic.GenerateEd25519Key(nil)

		r := NewRepublisher(pub, dbKs, selfKey, svc.decryptPrivateKey, zaptest.NewLogger(t))

		err := r.republishAll(context.Background())
		assert.NoError(tb, err, "republishAll returns nil even when individual keys fail (logs error)")
	}, republisherTestOptions)
}

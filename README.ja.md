<!-- i18n: language-switcher -->
[English](README.md) | [日本語](README.ja.md)

# ArangoDBコネクタ

ArangoDB用のネイティブIrodoriテーブルコネクタ拡張機能。

このクレートは、コネクタのメタデータ、ネイティブABIエクスポート、およびIrodori拡張マーケットプレイスで使用されるドライバ実装をパッケージ化しています。

## コネクタ

- 拡張機能ID: `irodori.arangodb`
- エンジンID: `arangodb`
- ワイヤープロトコル: `graph`
- デフォルトポート: `8529`
- ネイティブABI: `irodori.connector.native.v1`
- ドライバ連携: `はい`
- マーケットプレイスの表示設定: `公開`
- パッケージバージョン: `0.1.3`

このパッケージはコネクタのメタデータとネイティブドライバを直接使用しており、デスクトップアダプターのスナップショットは必要ありません。

コネクタのメタデータは `connector.config.json` と `irodori.extension.json` に格納されています。
Rustクレートは `src/lib.rs` からネイティブABIをエクスポートし、`irodori-connector-abi` を共有JSON/バッファヘルパーとして使用し、コネクタの動作は `src/driver.rs` に保持しています。

## 接続メタデータ

- エンドポイントモード: `hostPort`, `connectionString`
- トランスポートモード: `direct`, `sshTunnel`, `socks5Proxy`, `httpConnectProxy`, `proxyChain`
- TLS対応: `はい`
- TLS必須（デフォルト）: `いいえ`
- カスタムドライバオプション: `はい`

### エンドポイントフィールド

| フィールド | ラベル | 型 | 必須 |
| --- | --- | --- | --- |
| `host` | ホスト | `string` | はい |
| `port` | ポート | `number` | いいえ |
| `database` | データベース | `string` | いいえ |

## 認証

コネクタはこれらの認証モードを公開しており、クライアントは適切な資格情報フィールドをレンダリングできます。必要に応じて、ドライバ固有またはプロバイダ固有の値も `options` を通じて渡すことが可能です。

| 認証方法 | ラベル | 種類 | シークレットの用途 |
| --- | --- | --- | --- |
| `none` | 認証なし | `none` | なし |
| `connectionString` | 接続文字列 / DSN | `connectionString` | なし |
| `basic` | ベーシック認証 | `userPassword` | `password` |
| `jwt` | JWTベアラトークン | `token` | `token` |
| `clientCertificate` | クライアント証明書 / mTLS | `certificate` | `privateKey`, `privateKeyPassphrase` |
| `customDriverOptions` | カスタムドライバオプション | `custom` | `password`, `token`, `privateKey`, `privateKeyPassphrase` |

## エクスペリエンスメタデータ

- ドメイン: `graph`
- 結果ビュー: `graph`, `path`, `table`, `json`
- オブジェクトタイプ: `graphs`, `vertexCollections`, `edgeCollections`, `indexes`, `analyzers`
- インスピレーション元: ArangoDB Webインターフェースのグラフビューア、AQLグラフトラバーサル、AQL最短経路

| ワークフロー | 結果ビュー | テンプレート |
| --- | --- | --- |
| 隣接ノードの探索 | `graph` | `graph-aql-neighborhood` |
| 最短経路 | `path` | `graph-aql-shortest-path` |
| コレクションサンプル | `table` | `graph-aql-sample` |

| テンプレート | ラベル | 言語 | 結果ビュー |
| --- | --- | --- | --- |
| `graph-aql-neighborhood` | 隣接探索 | `aql` | `graph` |
| `graph-aql-shortest-path` | 最短経路 | `aql` | `path` |
| `graph-aql-sample` | コレクションサンプル | `aql` | `table` |

## ネイティブABI呼び出し

| メソッド | 応答 |
| --- | --- |
| `health` | コネクタのヘルス状態、エンジンID、ABIバージョン、ドライバの状態を返します。 |
| `describe` | 埋め込みマニフェストとコネクタ設定を返します。 |
| `manifest` | 生の `irodori.extension.json` を返します。 |
| `config` | 生の `connector.config.json` を返します。 |
| `connect` | ネイティブコネクタ接続を開き、検証します。 |
| `query` | コネクタクエリを実行し、構造化された行またはJSON結果を返します。 |
| `metadata` | スキーマ、テーブル、カラム、インデックス、コレクション、または同等のメタデータを読み取ります。 |
| `close` | キャッシュされたネイティブ接続を閉じて削除します。 |

## 開発

このチェックアウト内のすべての拡張クレートは `../target` を共有しており、依存関係は兄弟リポジトリ間で一度だけコンパイルされます。

```sh
make check
make build
```

リリースパッケージはプラットフォーム固有のネイティブアーティファクトを `dist/native` に配置します。

## ライセンス

0BSD。ほぼすべての目的でこのプロジェクトを使用、コピー、修正、配布できます。
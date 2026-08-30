# BUZZ NOW V11 Launch

## 0円スタート
`render.yaml` を同梱。
GitHubへ置き、Render Blueprint/Web Serviceから公開できる構成。

## 公開時に設定するもの
- SITE_URL=https://実際のドメイン
- DEMO_MODE=false
- REAL_DATA_MODE=true

## 広告
最初はOFF。
審査・提携後に環境変数だけでONにする。

AdSense:
- ADSENSE_ENABLED=true
- ADSENSE_CLIENT=承認済みclient ID

成果報酬:
- AFFILIATE_ENABLED=true
- AFFILIATE_PROVIDER=provider name
- AFFILIATE_SCRIPT_URL=契約先が指定する正規script URL

記事ごとの手貼りを避けるため、BUZZ NOW側は
Monetize Score → intent → recommended mode
まで自動判定する。

## Monetize Score
検索需要と商業意図を分けて評価。

S: 85+
A: 72+
B: 58+
C: under 58

候補モード:
- adsense
- affiliate
- hybrid

## 重要
広告を誤クリックさせるUIにはしない。
広告枠は明示し、成果報酬が有効な場合はPR表示を出す。

## DB
V11は既存コードとの互換性を優先してSQLiteを維持。
無料公開・初期検証用。

アクセスが発生し始めた時点でDB adapterをPostgreSQLへ切り替える。
URL/SEOページ構造は変えずに移行できるよう、DBを画面から分離した構成を維持する。

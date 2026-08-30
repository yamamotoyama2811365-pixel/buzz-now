# BUZZ NOW V12 公開チェックリスト

1. GitHubで `buzz-now` リポジトリを作成
2. このZIPの中身をリポジトリ直下へアップロード
3. RenderでGitHubを接続
4. `render.yaml` を使ってWeb Serviceを作成
5. Renderが発行したURLを `SITE_URL` に設定
6. Deploy
7. `/health` と `/ready` を確認
8. `/robots.txt` と `/sitemap.xml` を確認
9. 最初は広告をOFFのまま運用
10. 24〜72時間、実データを蓄積
11. Search Console と GA4 を接続
12. 独自ドメイン設定後、`SITE_URL` を独自ドメインへ変更

V12は初期検証用としてSQLiteを維持しています。
アクセスが出た段階で永続DBへ移行しても、SEOページURLは変えない方針です。

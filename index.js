// 必要なライブラリの読み込み
const express = require('express');
const { Pool } = require('pg');

const app = express();

// JSONボディの受信を可能にするミドルウェア
app.use(express.json());

// PostgreSQLとの接続設定（Render上の環境変数 DATABASE_URL を使用）
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false } // RenderではSSLが必要
});

// GASなどから送信されたJSONデータを受け取るエンドポイント
app.post('/api/orders/upload', async (req, res) => {
  const rows = req.body.data; // data配列としてJSONで送られてくる受注データ
  const client = await pool.connect();

  try {
    await client.query('BEGIN'); // トランザクション開始

    for (const row of rows) {
      // JSONオブジェクトから必要な項目を取り出す
      const {
        order_id, order_date, sales_dept, customer_name, customer_id,
        product_code, product_name, quantity, unit_price, total_price,
        currency, delivery_date, order_status, jpy_value
      } = row;

      // 注文ステータスが空でない（キャンセル・変更など）場合は、同じorder_idのデータを削除
      if (order_status && order_status.trim() !== "") {
        await client.query(`DELETE FROM orders WHERE order_id = $1`, [order_id]);
      }

      // 新規データとして追加（削除済みデータも含む）
      await client.query(`
        INSERT INTO orders (
          order_id, order_date, sales_dept, customer_name, customer_id,
          product_code, product_name, quantity, unit_price, total_price,
          currency, delivery_date, order_status, jpy_value
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
      `, [
        order_id, order_date, sales_dept, customer_name, customer_id,
        product_code, product_name, quantity, unit_price, total_price,
        currency, delivery_date, order_status, jpy_value
      ]);
    }

    await client.query('COMMIT'); // コミットしてDBに反映
    res.status(200).send('DB登録完了'); // 成功レスポンス
  } catch (err) {
    await client.query('ROLLBACK'); // エラー時はロールバック
    console.error(err);
    res.status(500).send('DBエラー');
  } finally {
    client.release(); // DB接続を解放
  }
});

// csv-stringifyをインポート
const { stringify } = require('csv-stringify');

// 今年度の受注データをCSVとしてエクスポートするAPIエンドポイント
app.get('/api/orders/export-current-fiscal-year', async (req, res) => {
  try {
    // 本日の日付から今年度を取得するロジック（日本式の年度）
    const today = new Date();
    const currentYear = today.getFullYear();
    const currentMonth = today.getMonth() + 1; // getMonth()は0から始まるため+1

    let fiscalYear;
    // 4月始まりの年度の場合 (4月～3月)
    if (currentMonth >= 4) {
      fiscalYear = `${currentYear}年度`;
    } else {
      fiscalYear = `${currentYear - 1}年度`;
    }

    // デバッグ用ログ
    console.log(`Exporting data for fiscal year: ${fiscalYear}`);

    // PostgreSQLから指定年度のデータを取得
    // ここでテーブル名ではなく、データベースのビュー名 (orders_view など) を指定してください
    const query = `
      SELECT *
      FROM orders_view
      WHERE fiscal_year = $1;
    `;
    const result = await pool.query(query, [fiscalYear]);
    const records = result.rows;

    if (records.length === 0) {
      console.log(`No records found for fiscal year: ${fiscalYear}`);
      return res.status(404).send(`No order data found for fiscal year ${fiscalYear}.`);
    }

    // CSVヘッダーの定義 (viewのカラム名に合わせて調整してください)
    const columns = Object.keys(records[0]); // 最初のレコードのキーをそのままヘッダーとして使用

    // CSV文字列に変換
    stringify(records, { header: true, columns: columns }, (err, csvString) => {
      if (err) {
        console.error('Error stringifying CSV:', err);
        return res.status(500).send('Error generating CSV data.');
      }

      // CSVファイルをダウンロードさせるためのヘッダーを設定
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', `attachment; filename="orders_fiscal_year_${fiscalYear}.csv"`);
      res.status(200).send(csvString);
      console.log(`Successfully exported ${records.length} records for ${fiscalYear}.`);
    });

  } catch (err) {
    console.error('Error exporting order data:', err.stack);
    res.status(500).send('Failed to export order data from database.');
  }
});

// テスト用エンドポイント（ブラウザでアクセスして動作確認可能）
app.get('/', (req, res) => {
  res.send('Order Import API (JSON)');
});

// サーバー起動（Renderでは環境変数PORTが設定されている）
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`サーバーがポート${PORT}で起動中`);
});

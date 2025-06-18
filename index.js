// /workspaces/order-api/index.js

const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json({ limit: '10mb' }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

app.post('/api/orders/upload', async (req, res) => {
  const rows = req.body.data;
  if (!rows || !Array.isArray(rows) || rows.length === 0) {
    return res.status(400).send('アップロードするデータが見つかりません。');
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // --- ▼▼▼ 変更点 1: INSERT処理の修正 ▼▼▼ ---
    // ON CONFLICT の対象を、データベースで設定した複合UNIQUE制約名に変更します。
    // これにより、「完全に同一のレコード」のみ挿入をスキップします。
    const insertQuery = `
      INSERT INTO orders (
        order_id, order_date, sales_dept, customer_name, customer_id,
        product_code, product_name, quantity, unit_price, total_price,
        currency, delivery_date, order_status, jpy_value, timestamp
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
      )
      -- ここでステップ1で設定した制約名を指定します
      ON CONFLICT ON CONSTRAINT uq_orders_composite_key DO NOTHING;
    `;
    
    for (const row of rows) {
      const values = [
        row.order_id, row.order_date, row.sales_dept, row.customer_name,
        row.customer_id, row.product_code, row.product_name, row.quantity,
        row.unit_price, row.total_price, row.currency, row.delivery_date,
        row.order_status, row.jpy_value, row.timestamp
      ];
      await client.query(insertQuery, values);
    }
    console.log(`${rows.length}件のデータ挿入処理（完全重複はスキップ）が完了しました。`);
    // --- ▲▲▲ 変更点 1: ここまで ▲▲▲ ---


    // --- ▼▼▼ 変更点 2: ウィンドウ関数の修正 ▼▼▼ ---
    // timestampが同じ場合を考慮し、order_statusで「最新」の状態を判断するロジックを追加します。
    console.log('is_countableカラムの更新処理を開始します...');
    const updateQuery = `
      WITH OrderStatusAnalysis AS (
          SELECT
              order_id,
              timestamp,
              customer_id,
              -- グループ内に'キャンセル'があるかの判定は変更なし
              MAX(CASE WHEN order_status = 'キャンセル' THEN 1 ELSE 0 END) OVER (PARTITION BY order_id) AS has_cancelled_in_group,
              
              -- 「最新のレコード」を特定するためのROW_NUMBER()を修正
              ROW_NUMBER() OVER (
                  PARTITION BY order_id 
                  ORDER BY 
                      timestamp DESC,
                      -- timestampが同じ場合はorder_statusで優先順位を決定
                      -- 1. キャンセル, 2. 変更, 3. それ以外(受注など) の順で「新しい」と判断
                      CASE order_status
                          WHEN 'キャンセル' THEN 1
                          WHEN '変更' THEN 2
                          ELSE 3
                      END
              ) AS rn_latest
          FROM
              orders
      )
      UPDATE orders
      SET
          is_countable =
              CASE
                  WHEN LEFT(orders.customer_id, 1) = 'Z' THEN false
                  WHEN OSA.has_cancelled_in_group = 1 THEN FALSE
                  -- rn_latest が 1 のレコード（＝最新の状態）のみをTRUEにする
                  WHEN OSA.has_cancelled_in_group = 0 AND OSA.rn_latest = 1 THEN TRUE
                  ELSE FALSE
              END 
      FROM OrderStatusAnalysis AS OSA
      WHERE orders.order_id = OSA.order_id
        AND orders.timestamp = OSA.timestamp;
    `;
    
    const result = await client.query(updateQuery);
    console.log(`is_countableカラムの更新が完了しました。影響を受けた行数: ${result.rowCount}`);
    // --- ▲▲▲ 変更点 2: ここまで ▲▲▲ ---

    await client.query('COMMIT');
    res.status(200).send('データベースへの追加・更新が正常に完了しました。');

  } catch (err) {
    await client.query('ROLLBACK');
    console.error('データベース処理中にエラーが発生しました:', err);

    res.status(500).send('データベース処理中にエラーが発生しました。詳細はサーバーログを確認してください。');
  } finally {
    client.release();
  }
});

// csv-stringifyをインポート
const { stringify } = require('csv-stringify');

// データベース接続テスト (オプション: サーバー起動時にログに出力)
pool.connect((err, client, release) => {
  if (err) {
    return console.error('Error acquiring client', err.stack);
  }
  console.log('Successfully connected to PostgreSQL database!');
  release();
});

// 今年度の受注データをCSVとしてエクスポートするAPIエンドポイント
app.get('/api/orders/export-current-fiscal-year', async (req, res) => {
  try {
    const today = new Date();
    const currentYear = today.getFullYear();
    const currentMonth = today.getMonth() + 1; // getMonth()は0-indexedのため+1

    // データベースクエリとログ用の会計年度文字列
    let fiscalYear;

    // 4月から新年度と仮定して会計年度を計算
    if (currentMonth >= 4) {
      fiscalYear = `${currentYear}年度`;
    } else {
      fiscalYear = `${currentYear - 1}年度`;
    }

    console.log(`Exporting data for fiscal year: ${fiscalYear}`);

    const query = `
      SELECT *
      FROM orders_view
      WHERE fiscal_year = $1;
    `;
    const result = await pool.query(query, [fiscalYear]); // fiscalYearを使用
    let records = result.rows;

    if (records.length === 0) {
      console.log(`No records found for fiscal year: ${fiscalYear}`);
      return res.status(404).send(`No order data found for fiscal year ${fiscalYear}.`);
    }

      // 日付列をYYYY-MM-DD形式の文字列に変換する
      const formattedRecords = records.map(record => {
      const newRecord = { ...record }; // 元のレコードを直接変更しないようにコピー

      // order_date 列を変換
      if (newRecord.order_date instanceof Date) {
        newRecord.order_date = newRecord.order_date.toISOString().split('T')[0];
      }
      // delivery_date 列を変換（もし存在し、Dateオブジェクトの場合）
      if (newRecord.delivery_date instanceof Date) {
        newRecord.delivery_date = newRecord.delivery_date.toISOString().split('T')[0];
      }
      // timestamp フィールドのフォーマット処理
      // newRecord.timestamp が存在し、かつ Date オブジェクトであることを確認
      if (newRecord.timestamp && newRecord.timestamp instanceof Date) {
      const date = newRecord.timestamp;

      // 各部分を取り出す
      const year = date.getFullYear();
      // 月は 0 から始まるため +1 し、2桁表示のためにpadStartを使う
      const month = (date.getMonth() + 1).toString().padStart(2, '0');
      const day = date.getDate().toString().padStart(2, '0');
      const hours = date.getHours().toString().padStart(2, '0');
      const minutes = date.getMinutes().toString().padStart(2, '0');
      const seconds = date.getSeconds().getSeconds().toString().padStart(2, '0'); // ここが誤りでした

      // 各部分を取り出す
      const year = date.getFullYear();
      // 月は 0 から始まるため +1 し、2桁表示のためにpadStartを使う
      const month = (date.getMonth() + 1).toString().padStart(2, '0');
      const day = date.getDate().toString().padStart(2, '0');
      const hours = date.getHours().toString().padStart(2, '0');
      const minutes = date.getMinutes().toString().padStart(2, '0');
      // 秒を取り出す（先ほどのコードでgetSecondsが抜けていました）
      const seconds = date.getSeconds().toString().padStart(2, '0');

      // 希望する形式に結合する
      // newRecord.timestamp プロパティ自体を上書きする場合
      newRecord.timestamp = `${year}${month}${day}${hours}:${minutes}:${seconds}`;

      // あるいは、フォーマット済みの値を別のプロパティに格納する場合（例: timestamp_formatted）
      // newRecord.timestamp_formatted = `${year}${month}${day}${hours}:${minutes}:${seconds}`;

      } else if (newRecord.timestamp != null) {
      // もし timestamp が Date オブジェクトでない、または null/undefined でない場合、
      // 何らかの理由で Date オブジェクトとして取得できなかった可能性がある
      console.warn("newRecord.timestamp is not a Date object or is null:", newRecord.timestamp);
      // 必要に応じて、元の値をそのまま使う、エラーを示す文字列にする、などの処理を検討
      newRecord.timestamp = 'Invalid Date';
      } else {
      // newRecord.timestamp が null または undefined の場合
      console.log("newRecord.timestamp is null or undefined.");
      // 必要に応じて、空文字列にする、特定の文字列にするなどの処理を検討
      newRecord.timestamp = '';
      }

      return newRecord;
    });

    records = formattedRecords; // フォーマット済みのレコードを使用する

    // records[0]が存在することが保証された後にcolumnsを定義
    const columns = Object.keys(records[0]);

    // stringifyのPromise化
    const csvString = await new Promise((resolve, reject) => {
      stringify(records, { header: true, columns: columns }, (err, resultCsv) => {
        if (err) {
          console.error('Error generating CSV string:', err);
          return reject(err);
        }
        resolve(resultCsv);
      });
    });

    // CSV文字列をGASに直接返すためのヘッダーを設定
    res.setHeader('Content-Type', 'text/csv; charset=utf-8'); // CSV形式であることを明示し、文字コードを指定

    res.status(200).send(csvString);
    console.log(`Successfully sent ${records.length} records for ${fiscalYear}.`); // ログメッセージを"sent"に変更

  } catch (err) {
    console.error('Error exporting order data or generating CSV:', err.stack);
    res.status(500).send('Error generating CSV data.');
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

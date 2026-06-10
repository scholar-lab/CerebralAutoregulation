
class DataLogger:
    """
    Thread-safe logger that persists raw and computed CA data.

    Directory layout
    ----------------
    output_dir/
        raw/
            ca_raw_YYYYMMDD_HHMMSS.csv    ← MAP, ICP, CPP per sample
        metrics/
            ca_metrics_YYYYMMDD_HHMMSS.csv ← PRx, CPPopt, CA state per update
        ca_monitor.db                      ← SQLite (if use_sqlite=True)
    """

    RAW_HEADER     = ["timestamp", "datetime_utc", "map_mmhg", "icp_mmhg", "cpp_mmhg"]
    METRICS_HEADER = ["timestamp", "datetime_utc", "map_mmhg", "icp_mmhg", "cpp_mmhg",
                      "prx", "prx_n_samples", "prx_window_s",
                      "cppopt_mmhg", "delta_cpp", "ca_state", "recommendation"]

    def __init__(
        self,
        output_dir: str | Path = "ca_data",
        use_sqlite: bool = True,
        rotate_hours: float = 24.0,
    ):
        self.output_dir   = Path(output_dir)
        self.use_sqlite   = use_sqlite
        self.rotate_hours = rotate_hours
        self._lock        = threading.Lock()
        self._raw_writer: Optional[csv.DictWriter]     = None
        self._met_writer: Optional[csv.DictWriter]     = None
        self._raw_file    = None
        self._met_file    = None
        self._db: Optional[sqlite3.Connection]         = None
        self._file_open_time: float                    = 0.0

        self.output_dir.joinpath("raw").mkdir(parents=True, exist_ok=True)
        self.output_dir.joinpath("metrics").mkdir(parents=True, exist_ok=True)
        self._open_files()
        if use_sqlite:
            self._init_sqlite()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def log_raw(self, ts: float, map_v: float, icp_v: float, cpp: float) -> None:
        self._maybe_rotate()
        row = {
            "timestamp":    round(ts, 3),
            "datetime_utc": _iso(ts),
            "map_mmhg":     round(map_v, 2),
            "icp_mmhg":     round(icp_v, 2),
            "cpp_mmhg":     round(cpp,   2),
        }
        with self._lock:
            self._raw_writer.writerow(row)
            self._raw_file.flush()
        if self._db:
            self._db_insert_raw(row)

    def log_metrics(
        self,
        ts: float,
        map_v: float,
        icp_v: float,
        cpp: float,
        prx: float,
        prx_n: int,
        prx_window_s: float,
        cppopt: Optional[float],
        delta_cpp: Optional[float],
        ca_state: str,
        recommendation: str,
    ) -> None:
        self._maybe_rotate()
        row = {
            "timestamp":    round(ts, 3),
            "datetime_utc": _iso(ts),
            "map_mmhg":     round(map_v, 2),
            "icp_mmhg":     round(icp_v, 2),
            "cpp_mmhg":     round(cpp,   2),
            "prx":          round(prx, 4),
            "prx_n_samples": prx_n,
            "prx_window_s": round(prx_window_s, 1),
            "cppopt_mmhg":  round(cppopt, 1) if cppopt is not None else "",
            "delta_cpp":    round(delta_cpp, 1) if delta_cpp is not None else "",
            "ca_state":     ca_state,
            "recommendation": recommendation,
        }
        with self._lock:
            self._met_writer.writerow(row)
            self._met_file.flush()
        if self._db:
            self._db_insert_metrics(row)

    def close(self) -> None:
        with self._lock:
            if self._raw_file:
                self._raw_file.close()
            if self._met_file:
                self._met_file.close()
            if self._db:
                self._db.close()

    # ------------------------------------------------------------------
    # File management
    # ------------------------------------------------------------------

    def _open_files(self) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        raw_path = self.output_dir / "raw"     / f"ca_raw_{stamp}.csv"
        met_path = self.output_dir / "metrics" / f"ca_metrics_{stamp}.csv"

        self._raw_file = open(raw_path, "w", newline="", buffering=1)
        self._met_file = open(met_path, "w", newline="", buffering=1)

        self._raw_writer = csv.DictWriter(self._raw_file, fieldnames=self.RAW_HEADER)
        self._met_writer = csv.DictWriter(self._met_file, fieldnames=self.METRICS_HEADER)
        self._raw_writer.writeheader()
        self._met_writer.writeheader()

        self._file_open_time = time.time()
        logger.info("Logging raw → %s", raw_path)
        logger.info("Logging metrics → %s", met_path)

    def _maybe_rotate(self) -> None:
        if time.time() - self._file_open_time > self.rotate_hours * 3600:
            with self._lock:
                if self._raw_file:
                    self._raw_file.close()
                if self._met_file:
                    self._met_file.close()
                self._open_files()

    # ------------------------------------------------------------------
    # SQLite
    # ------------------------------------------------------------------

    def _init_sqlite(self) -> None:
        db_path = self.output_dir / "ca_monitor.db"
        self._db = sqlite3.connect(str(db_path), check_same_thread=False)
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS raw_samples (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   REAL NOT NULL,
                datetime_utc TEXT,
                map_mmhg    REAL,
                icp_mmhg    REAL,
                cpp_mmhg    REAL
            )
        """)
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS ca_metrics (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp    REAL NOT NULL,
                datetime_utc TEXT,
                map_mmhg     REAL,
                icp_mmhg     REAL,
                cpp_mmhg     REAL,
                prx          REAL,
                prx_n_samples INTEGER,
                prx_window_s REAL,
                cppopt_mmhg  REAL,
                delta_cpp    REAL,
                ca_state     TEXT,
                recommendation TEXT
            )
        """)
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_raw_ts ON raw_samples(timestamp)")
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_met_ts ON ca_metrics(timestamp)")
        self._db.commit()
        logger.info("SQLite database: %s", db_path)

    def _db_insert_raw(self, row: dict) -> None:
        try:
            self._db.execute(
                "INSERT INTO raw_samples (timestamp,datetime_utc,map_mmhg,icp_mmhg,cpp_mmhg) "
                "VALUES (?,?,?,?,?)",
                (row["timestamp"], row["datetime_utc"],
                 row["map_mmhg"], row["icp_mmhg"], row["cpp_mmhg"])
            )
            self._db.commit()
        except Exception as exc:
            logger.warning("SQLite raw insert error: %s", exc)

    def _db_insert_metrics(self, row: dict) -> None:
        try:
            self._db.execute(
                "INSERT INTO ca_metrics "
                "(timestamp,datetime_utc,map_mmhg,icp_mmhg,cpp_mmhg,"
                "prx,prx_n_samples,prx_window_s,cppopt_mmhg,delta_cpp,"
                "ca_state,recommendation) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    row["timestamp"], row["datetime_utc"],
                    row["map_mmhg"], row["icp_mmhg"], row["cpp_mmhg"],
                    row["prx"], row["prx_n_samples"], row["prx_window_s"],
                    row["cppopt_mmhg"] or None,
                    row["delta_cpp"]   or None,
                    row["ca_state"],
                    row["recommendation"],
                )
            )
            self._db.commit()
        except Exception as exc:
            logger.warning("SQLite metrics insert error: %s", exc)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

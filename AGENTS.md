# Agent Guidelines & Project Conventions

This document outlines the preferred coding style, UX patterns, and architectural conventions for the HackerBook project. Agents should follow these guidelines to ensure consistency and high-quality output.

## User Experience (UX) & CLI Output

*   **"Easy Mode" Philosophy:** The console output should be verbose enough to be informative but structured enough to be readable. The user should never wonder "is it stuck?".
*   **Progress Tracking:**
    *   **Phases:** Break complex jobs into clear, numbered phases (e.g., `Phase 1/6: Opening DB`).
    *   **Spinners:** Always use a spinner for active states. Ensure it animates smoothly (target ~10Hz).
    *   **Granularity:** Display `current/total`, percentage, rate (items/sec), and ETA whenever possible.
    *   **Context:** Provide summary statistics early (e.g., "Opened DB - 22GB, 2 tables").
    *   **Responsiveness:** The UI must **never freeze**.
        *   Use `setImmediate()` or `process.nextTick()` to yield to the event loop during tight loops.
        *   Offload blocking synchronous operations (like heavy SQLite queries) to **Worker Threads**.
        *   Use background timers to keep UI elements (spinners, clocks) updating during async waits.
    *   **Clean Output:** Use ANSI escape codes (e.g., `\x1b[K`) to clear lines when updating progress to prevent visual artifacts.

## Architecture & Performance

*   **Database (SQLite):**
    *   Use `better-sqlite3`.
    *   Optimize pragmas for bulk operations (e.g., `journal_mode = OFF`, `synchronous = OFF`, large `cache_size`).
    *   Avoid blocking the main thread with long-running synchronous queries.
*   **Async/Await:**
    *   Prefer `fs/promises` for file operations.
    *   Use `async` functions for main flows.
*   **Modularity:**
    *   Keep scripts focused.
    *   Extract reusable utilities (like the progress tracker) if they grow too large, or keep them self-contained if small.

## Code Style

*   **Format:** ES Modules (`.mjs` or `type: module` in package.json).
*   **Edits:** Make surgical, minimal changes. Don't rewrite entire files unless necessary.
*   **Safety:** Always validate inputs and handle errors gracefully (e.g., `try/catch` blocks around file/DB ops).

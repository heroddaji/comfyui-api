import chokidar, { FSWatcher } from "chokidar";
import path from "path";
import fs, { Stats } from "fs";

// Function to check if the file has stopped changing
function waitForFileStability(filePath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    let lastKnownSize = -1;
    let retries = 0;
    let attempts = 0;
    const maxDelayMs = 50 * 20 * 30; // max Delay is 30 seconds
    const delayMs = 50;

    const checkFile = () => {
      fs.stat(filePath, (err, stats) => {
        if (err) {
          console.error(`Error accessing file: ${err}`);
          reject(err);
          return;
        }

        if (stats.size > 0 && stats.size === lastKnownSize) {
          if (retries >= 3) {
            // Consider the file stable after 3 checks
            resolve();
          } else {
            retries++;
            setTimeout(checkFile, delayMs);
          }
        } else {
          lastKnownSize = stats.size;
          retries = 0;
          attempts++;
          
          if (attempts * delayMs >= maxDelayMs) {
            resolve();
            return;
          }
          setTimeout(checkFile, delayMs);
        }
      });
    };

    checkFile();
  });
}

export class DirectoryWatcher {
  private watcher: FSWatcher | null = null;
  private directory: string;
  private activeTasks: Set<Promise<any>> = new Set();
  private prefixActions: Record<string, (path: string) => Promise<void>> = {};
  private prefixActionCounts: Record<string, number> = {};

  constructor(directory: string) {
    this.directory = directory;
    console.log(`Watching directory: ${this.directory}`);
    this.watcher = chokidar.watch(this.directory, {
      ignored: /^\./,
      persistent: true,
    });

    this.watcher.on("add", async (filepath: string, stats?: Stats) => {
      for (const prefix in this.prefixActions) {
        if (path.basename(filepath).startsWith(prefix)) {
          try {
            await waitForFileStability(filepath);
            const task = this.prefixActions[prefix](filepath).finally(() => {
              this.activeTasks.delete(task);
              if (this.prefixActionCounts[prefix] > 0) {
                this.prefixActionCounts[prefix]--;
              }
              if (this.prefixActionCounts[prefix] === 0) {
                this.removePrefixAction(prefix);
              }
            });
            this.activeTasks.add(task);
          } catch (error) {
            console.error(`Error processing file ${filepath}: ${error}`);
          }
        }
      }
    });
  }

  addPrefixAction(
    prefix: string,
    maxExecutions: number,
    action: (path: string) => Promise<void>
  ) {
    this.prefixActions[prefix] = action;
    this.prefixActionCounts[prefix] = maxExecutions;
  }

  removePrefixAction(prefix: string) {
    delete this.prefixActions[prefix];
  }

  // Stop watching the directory
  async stopWatching(): Promise<void> {
    if (this.watcher) {
      await Promise.all(this.activeTasks); // Wait for all tasks to complete
      console.log("All tasks completed.");
      this.watcher.close(); // Close the watcher to free up resources
      this.watcher = null;
      console.log("Stopped watching directory.");
    }
  }
}

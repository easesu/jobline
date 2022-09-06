import chalk from 'chalk';

export type JobName = string;

export interface JobContext {
  [key: string]: any;
}

export interface JobExecuteResult {
  output: any;
  ctx: JobContext;
  journal: JobJournal;
  done?: boolean;
}

export type JobExecutor = (
  ctx: JobContext,
  args: Record<string, any>,
  options: {
    done: () => void;
    log: (...args: any[]) => void;
  }
) => any;

export interface Job {
  name: JobName;
  label?: string;
  desc?: string;
  args?: string[];
  executor: JobExecutor;
}

export interface JobLineStep {
  name: JobName;
  inheritable?: boolean;
  args?: ([string, any] | [string, number, string] | [string, 'external', any] | [string, symbol, any])[],
}

export interface JobLine {
  name: string;
  args?: string[],
  steps: JobLineStep[];
}

export interface JobJournal {
  name: JobName;
  input: Record<string, any>;
  output: any;
  status?: 'pending' | 'exception' | 'done'
}

export interface JobLineJournal {
  name: string;
  input: Record<string, any>;
  status?: 'pending' | 'exception' | 'done'
  steps: JobJournal[];
}

export const PREVIOUS_STEP = Symbol('previous');

const jobs: Map<JobName, Job> = new Map();
const jobLines: Map<string, JobLine> = new Map();

export function registerJob(job: Job) {
  if (!job) {
    return;
  }
  const { name } = job;
  if (jobs.get(name)) {
    throw new Error(`job ${name} 已存在`);
  }
  jobs.set(name, job);
}

export function registerJobLine(jobLine: JobLine) {
  if (!jobLine) {
    return;
  }
  const { name } = jobLine;
  if (jobLines.get(name)) {
    throw new Error(`jobLine ${name} 已存在`);
  }
  jobLines.set(name, jobLine);
}

function log(...args: any[]) {
  console.log(chalk.cyan('  🔔'), ...args);
}

async function executeOneJob(job: Job, ctx: JobContext | null, args: Record<string, any>, journal?: JobJournal): Promise<JobExecuteResult> {
  let jobJournal: JobJournal;
  if (journal) {
    jobJournal = {
      ...journal
    };
  } else {
    jobJournal = {
      name: job.name,
      input: args,
      output: null,
      status: 'pending',
    };
  }
  let jobContext: JobContext;
  if (ctx) {
    jobContext = ctx;
  } else {
    jobContext = {};
  }
  let jobOutput = undefined;
  console.log(chalk.yellowBright('🟡 执行任务:', chalk.green(job.label || job.name)));
  let isDone = false;
  const done = () => {
    isDone = true;
  }
  try {
    jobOutput = await job.executor(jobContext, args, {
      done,
      log,
    });
    jobJournal.output = jobOutput;
    jobJournal.status = 'done';
  } catch (err) {
    console.error(err);
    jobJournal.status = 'exception';
  }
  return {
    ctx: jobContext,
    journal: jobJournal,
    output: jobOutput,
    done: isDone,
  };
}

function buildJobArgs(args: JobLineStep['args'], jobArgs: undefined | string[], options: {
  external?: Record<string, any>;
  previous?: Record<string, any>;
  steps: JobLineJournal['steps'];
  index: number;
}) {
  const res: Record<string, any> = {};
  if (!args || !jobArgs) {
    return res;
  }

  const assignedArgs: string[] = [];
  args.forEach((arg) => {
    if (!Array.isArray(arg)) {
      return;
    }
    const argKey = arg[0];
    if (!jobArgs.includes(argKey)) {
      return;
    }
    if (arg.length === 2) {
      res[argKey] = arg[1];
      assignedArgs.push(argKey);
    } else if (arg.length === 3) {
      const type = arg[1];
      if (type === 'external') {
        if (options.external) {
          res[argKey] = options.external[arg[2]];
          assignedArgs.push(argKey);
        }
      } else if (type === PREVIOUS_STEP) {
        if (options.previous) {
          res[argKey] = options.previous[arg[2]];
          assignedArgs.push(argKey);
        }
      } else if (typeof type === 'number') {
        if (options.steps) {
          let stepOutput;
          if (type < 0) {
            if (options.steps[options.index + type]) {
              stepOutput = options.steps[options.index + type].output;
            }
          } else {
            if (options.steps[type]) {
              stepOutput = options.steps[type].output;
            }
          }
          if (stepOutput) {
            res[argKey] = stepOutput[arg[2]];
            assignedArgs.push(argKey);
          }
        }
      }
    }
  });

  if (options.external && assignedArgs.length < jobArgs.length) {
    jobArgs.forEach(jobArg => {
      if (!assignedArgs.includes(jobArg)) {
        res[jobArg] = options.external![jobArg];
      }
    });
  }

  return res;
}

export async function executeJobLine(jobLineName: string | JobLine, options?: {
  args?: Record<string, any>;
  previousJobLineJournal?: JobLineJournal;
}) {
  let jobLine: JobLine | undefined;
  if (typeof jobLineName === 'string') {
    jobLine = jobLines.get(jobLineName);
  } else {
    jobLine = jobLineName;
  }
  if (!jobLine) {
    console.log(chalk.bgRed(chalk.whiteBright(' 流水线 %s 不存在 ')), chalk.bold(chalk.whiteBright(jobLineName)));
    process.exit();
    return;
  }

  const externalArgs = {};

  if (jobLine.args) {
    const lackedArgs: string[] = [];
    jobLine.args.forEach(argKey => {
      const value = options?.args![argKey];
      if (value === undefined) {
        lackedArgs.push(argKey);
      } else {
        externalArgs[argKey] = value;
      }
    });
    if (lackedArgs.length > 0) {
      console.log(
        chalk.bgRed(chalk.whiteBright(' 流水线 %s 缺少参数 %s ')),
        chalk.bold(chalk.whiteBright(jobLine.name)),
        chalk.bold(chalk.whiteBright(lackedArgs.join('、')))
      );
      process.exit();
    }
  }

  const jobLineJournal: JobLineJournal = {
    name: jobLine.name,
    status: 'pending',
    input: externalArgs,
    steps: []
  };

  const previousJobLineJournal: JobLineJournal = options?.previousJobLineJournal || {
    name: jobLine.name,
    input: externalArgs,
    steps: []
  };

  let previousContext: JobContext | null = null;
  console.log(chalk.yellowBright('🔴🟡🟢 开始执行'));
  for (let index = 0; index < jobLine.steps.length; index++) {
    const step = jobLine.steps[index];
    const previousJournal = previousJobLineJournal.steps[index];
    if (previousJournal && previousJournal.status === 'done') {
      break;
    }
    const job = jobs.get(step.name);
    if (!job) {
      throw new Error(`任务 ${step.name} 不存在`);
    }
    const args = buildJobArgs(step.args, job.args, {
      external: externalArgs,
      previous: (jobLineJournal.steps[jobLineJournal.steps.length - 1] || {}).output,
      steps: jobLineJournal.steps,
      index,
    });
    const res = await executeOneJob(
      job,
      step.inheritable !== false ? previousContext : null,
      args,
      previousJournal,
    );
    jobLineJournal.steps[index] = res.journal;
    if (res.journal.status !== 'done') {
      jobLineJournal.status = res.journal.status;
      break;
    }
    if (res.done) {
      break;
    }
  }
  if (jobLineJournal.status === 'pending') {
    jobLineJournal.status = 'done';
  }
  if(jobLineJournal.status === 'exception') {
    console.log(chalk.yellowBright('🔴 执行出现错误, 请检查'));
  } else if(jobLineJournal.status === 'done') {
    console.log(chalk.yellowBright('🟢 执行结束'));
  }
  return jobLineJournal;
}

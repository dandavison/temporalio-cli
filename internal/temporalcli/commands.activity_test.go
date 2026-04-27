package temporalcli_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
)

const (
	activityId string = "dev-activity-id"
	identity   string = "MyIdentity"
)

func (s *SharedServerSuite) TestActivity_Complete() {
	run := s.waitActivityStarted()
	wid := run.GetID()
	res := s.Execute(
		"activity", "complete",
		"--activity-id", activityId,
		"--workflow-id", wid,
		"--result", "\"complete-activity-result\"",
		"--identity", identity,
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	var actual string
	s.NoError(run.Get(s.Context, &actual))
	s.Equal("complete-activity-result", actual)

	started, completed, failed := s.getActivityEvents(wid, activityId)
	s.NotNil(started)
	s.Nil(failed)
	s.NotNil(completed)
	s.Equal("\"complete-activity-result\"", string(completed.Result.Payloads[0].GetData()))
	s.Equal(identity, completed.GetIdentity())
}

func (s *SharedServerSuite) TestActivity_Fail() {
	run := s.waitActivityStarted()
	wid := run.GetID()
	detail := "{\"myKey\": \"myValue\"}"
	reason := "MyReason"
	identity := "MyIdentity"
	res := s.Execute(
		"activity", "fail",
		"--activity-id", activityId,
		"--workflow-id", wid,
		"--run-id", run.GetRunID(),
		"--detail", detail,
		"--reason", reason,
		"--identity", identity,
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	err := run.Get(s.Context, nil)
	s.NotNil(err)

	started, completed, failed := s.getActivityEvents(wid, activityId)
	s.NotNil(started)
	s.Nil(completed)
	s.NotNil(failed)
	s.Equal(
		detail,
		string(failed.GetFailure().GetApplicationFailureInfo().GetDetails().Payloads[0].GetData()),
	)
	s.Equal(reason, failed.GetFailure().Message)
	s.Equal(identity, failed.GetIdentity())
}

func (s *SharedServerSuite) TestActivity_Complete_InvalidResult() {
	run := s.waitActivityStarted()
	res := s.Execute(
		"activity", "complete",
		"--activity-id", activityId,
		"--workflow-id", run.GetID(),
		"--result", "{not json}",
		"--address", s.Address(),
	)
	s.ErrorContains(res.Err, "is not valid JSON")

	started, completed, failed := s.getActivityEvents(run.GetID(), activityId)
	s.Nil(started)
	s.Nil(completed)
	s.Nil(failed)
}

func (s *SharedServerSuite) TestActivity_Fail_InvalidDetail() {
	run := s.waitActivityStarted()
	wid := run.GetID()
	res := s.Execute(
		"activity", "fail",
		"--activity-id", activityId,
		"--workflow-id", wid,
		"--detail", "{not json}",
		"--address", s.Address(),
	)
	s.ErrorContains(res.Err, "is not valid JSON")

	started, completed, failed := s.getActivityEvents(wid, activityId)
	s.Nil(started)
	s.Nil(completed)
	s.Nil(failed)
}

func (s *SharedServerSuite) TestActivityOptionsUpdate_Accept() {
	run := s.waitActivityStarted()
	wid := run.GetID()

	res := s.Execute(
		"activity", "update-options",
		"--activity-id", activityId,
		"--workflow-id", wid,
		"--run-id", run.GetRunID(),
		"--identity", identity,
		"--task-queue", "new-task-queue",
		"--schedule-to-close-timeout", "60s",
		"--schedule-to-start-timeout", "5s",
		"--start-to-close-timeout", "10s",
		"--heartbeat-timeout", "20s",
		"--retry-initial-interval", "5s",
		"--retry-maximum-interval", "60s",
		"--retry-backoff-coefficient", "2",
		"--retry-maximum-attempts", "5",
		"--address", s.Address(),
	)

	s.NoError(res.Err)
	out := res.Stdout.String()
	s.ContainsOnSameLine(out, "ScheduleToCloseTimeout", "1m0s")
	s.ContainsOnSameLine(out, "ScheduleToStartTimeout", "5s")
	s.ContainsOnSameLine(out, "StartToCloseTimeout", "10s")
	s.ContainsOnSameLine(out, "HeartbeatTimeout", "10s")
	s.ContainsOnSameLine(out, "InitialInterval", "5s")
	s.ContainsOnSameLine(out, "MaximumInterval", "1m0s")
	s.ContainsOnSameLine(out, "BackoffCoefficient", "2")
	s.ContainsOnSameLine(out, "MaximumAttempts", "5")
}

func (s *SharedServerSuite) TestActivityOptionsUpdate_Partial() {
	run := s.waitActivityStarted()

	res := s.Execute(
		"activity", "update-options",
		"--activity-id", activityId,
		"--workflow-id", run.GetID(),
		"--run-id", run.GetRunID(),
		"--identity", identity,
		"--task-queue", "new-task-queue",
		"--schedule-to-close-timeout", "41s",
		"--schedule-to-start-timeout", "11s",
		"--retry-initial-interval", "4s",
		"--retry-maximum-attempts", "10",
		"--address", s.Address(),
	)

	s.NoError(res.Err)
	out := res.Stdout.String()

	// updated
	s.ContainsOnSameLine(out, "ScheduleToCloseTimeout", "41s")
	s.ContainsOnSameLine(out, "ScheduleToStartTimeout", "11s")
	s.ContainsOnSameLine(out, "StartToCloseTimeout", "10s")
	s.ContainsOnSameLine(out, "InitialInterval", "4s")
	s.ContainsOnSameLine(out, "MaximumAttempts", "10")

	// old value
	// note - this is a snapshot of current values
	// if this test fails, check the default values of activity options
	s.ContainsOnSameLine(out, "StartToCloseTimeout", "10s")
	s.ContainsOnSameLine(out, "HeartbeatTimeout", "0s")
	s.ContainsOnSameLine(out, "MaximumInterval", "1m40s")
	s.ContainsOnSameLine(out, "BackoffCoefficient", "2")
}

func sendActivityCommand(command string, run client.WorkflowRun, s *SharedServerSuite, extraArgs ...string) *CommandResult {
	args := []string{
		"activity", command,
		"--workflow-id", run.GetID(),
		"--run-id", run.GetRunID(),
		"--identity", identity,
		"--address", s.Address(),
	}

	args = append(args, extraArgs...)

	res := s.Execute(args...)
	return res
}

func (s *SharedServerSuite) TestActivityPauseUnpause() {
	run := s.waitActivityStarted()

	res := sendActivityCommand("pause", run, s, "--activity-id", activityId)
	s.NoError(res.Err)

	s.Eventually(func() bool {
		resp, err := s.Client.DescribeWorkflowExecution(s.Context, run.GetID(), run.GetRunID())
		s.NoError(err)
		if resp.GetPendingActivities() == nil {
			return false
		}
		return len(resp.PendingActivities) > 0 && resp.PendingActivities[0].Paused
	}, 5*time.Second, 100*time.Millisecond)

	res = sendActivityCommand("unpause", run, s, "--activity-id", activityId, "--reset-attempts")
	s.NoError(res.Err)

	s.Eventually(func() bool {
		resp, err := s.Client.DescribeWorkflowExecution(s.Context, run.GetID(), run.GetRunID())
		s.NoError(err)
		if resp.GetPendingActivities() == nil {
			return false
		}
		return len(resp.PendingActivities) > 0 && !resp.PendingActivities[0].Paused
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *SharedServerSuite) TestActivityCommandFailed_NoActivityId() {
	run := s.waitActivityStarted()

	// pause is single-workflow only
	res := sendActivityCommand("pause", run, s)
	s.ErrorContains(res.Err, "Activity Id must be specified")

	// unpause and reset support both single-workflow and batch modes
	for _, command := range []string{"unpause", "reset"} {
		res = sendActivityCommand(command, run, s)
		s.ErrorContains(res.Err, "either --activity-id and --workflow-id, or --query must be set")
	}
}

func (s *SharedServerSuite) TestActivityCommandFailed_BothWorkflowIdAndQuery() {
	run := s.waitActivityStarted()

	// unpause and reset support both single-workflow (--workflow-id) and batch
	// (--query) modes; providing both at once should fail.
	commands := []string{"unpause", "reset"}
	for _, command := range commands {
		res := s.Execute(
			"activity", command,
			"--workflow-id", run.GetID(),
			"--query", "WorkflowType='DevWorkflow'",
			"--activity-id", activityId,
			"--address", s.Address(),
		)
		s.ErrorContains(res.Err, "cannot set query when workflow ID is set")
	}
}

func (s *SharedServerSuite) TestActivityReset() {
	run := s.waitActivityStarted()

	res := sendActivityCommand("reset", run, s, "--activity-id", activityId)
	s.NoError(res.Err)
	// make sure we receive a server response
	out := res.Stdout.String()
	s.ContainsOnSameLine(out, "ServerResponse", "true")

	// reset should fail because activity is not found
	res = sendActivityCommand("reset", run, s, "--activity-id", "fake_id")
	s.Error(res.Err)
	// make sure we receive a NotFound error from the server`
	var notFound *serviceerror.NotFound
	s.ErrorAs(res.Err, &notFound)
}

// Test helpers

func (s *SharedServerSuite) waitActivityStarted() client.WorkflowRun {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		time.Sleep(0xFFFF * time.Hour)
		return nil, nil
	})
	run, err := s.Client.ExecuteWorkflow(
		s.Context,
		client.StartWorkflowOptions{TaskQueue: s.Worker().Options.TaskQueue},
		DevWorkflow,
		"ignored",
	)
	s.NoError(err)
	s.Eventually(func() bool {
		resp, err := s.Client.DescribeWorkflowExecution(s.Context, run.GetID(), run.GetRunID())
		s.NoError(err)
		return len(resp.PendingActivities) > 0
	}, 5*time.Second, 100*time.Millisecond)
	return run
}

func waitWorkflowStarted(s *SharedServerSuite) client.WorkflowRun {
	run, err := s.Client.ExecuteWorkflow(
		s.Context,
		client.StartWorkflowOptions{TaskQueue: s.Worker().Options.TaskQueue},
		DevWorkflow,
		"ignored",
	)
	s.NoError(err)
	s.Eventually(func() bool {
		resp, err := s.Client.DescribeWorkflowExecution(s.Context, run.GetID(), run.GetRunID())
		s.NoError(err)
		return len(resp.PendingActivities) > 0
	}, 5*time.Second, 100*time.Millisecond)
	return run
}

func (s *SharedServerSuite) getActivityEvents(workflowID, activityID string) (
	started *history.ActivityTaskStartedEventAttributes,
	completed *history.ActivityTaskCompletedEventAttributes,
	failed *history.ActivityTaskFailedEventAttributes,
) {
	iter := s.Client.GetWorkflowHistory(s.Context, workflowID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		s.NoError(err)
		if attrs := event.GetActivityTaskStartedEventAttributes(); attrs != nil {
			started = attrs
		} else if attrs := event.GetActivityTaskCompletedEventAttributes(); attrs != nil {
			completed = attrs
			s.Equal("json/plain", string(completed.Result.Payloads[0].Metadata["encoding"]))
		} else if attrs := event.GetActivityTaskFailedEventAttributes(); attrs != nil {
			failed = attrs
		}
	}
	return started, completed, failed
}

func checkActivitiesRunning(s *SharedServerSuite, run client.WorkflowRun) {
	s.Eventually(func() bool {
		resp, err := s.Client.DescribeWorkflowExecution(s.Context, run.GetID(), run.GetRunID())
		s.NoError(err)
		return len(resp.GetPendingActivities()) > 0
	}, 5*time.Second, 200*time.Millisecond)
}

func checkActivitiesPaused(s *SharedServerSuite, run client.WorkflowRun) {
	s.Eventually(func() bool {
		resp, err := s.Client.DescribeWorkflowExecution(s.Context, run.GetID(), run.GetRunID())
		s.NoError(err)
		if resp.GetPendingActivities() == nil {
			return false
		}
		return len(resp.GetPendingActivities()) > 0 && resp.GetPendingActivities()[0].Paused
	}, 5*time.Second, 200*time.Millisecond)
}

func (s *SharedServerSuite) TestUnpauseActivity_BatchSuccess() {
	var failActivity atomic.Bool
	failActivity.Store(true)
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		if failActivity.Load() {
			return nil, fmt.Errorf("update workflow received non-float input")
		}
		return nil, nil
	})

	s.Worker().OnDevWorkflow(func(ctx workflow.Context, a any) (any, error) {
		// override the activity options to allow activity to constantly fail
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:          activityId,
			StartToCloseTimeout: 1 * time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 0,
			},
		})
		var res any
		err := workflow.ExecuteActivity(ctx, DevActivity).Get(ctx, &res)
		return res, err
	})

	run1 := waitWorkflowStarted(s)
	run2 := waitWorkflowStarted(s)

	// Wait for all to appear in list
	query := fmt.Sprintf("WorkflowId = '%s' OR WorkflowId = '%s'", run1.GetID(), run2.GetID())
	s.Eventually(func() bool {
		resp, err := s.Client.ListWorkflow(s.Context, &workflowservice.ListWorkflowExecutionsRequest{
			Query: query,
		})
		s.NoError(err)
		return len(resp.Executions) == 2
	}, 3*time.Second, 100*time.Millisecond)

	// Pause the activities
	res := sendActivityCommand("pause", run1, s, "--activity-id", activityId)
	s.NoError(res.Err)
	res = sendActivityCommand("pause", run2, s, "--activity-id", activityId)
	s.NoError(res.Err)

	// wait for activities to be paused
	checkActivitiesPaused(s, run1)
	checkActivitiesPaused(s, run2)

	var lastRequestLock sync.Mutex
	var startBatchRequest *workflowservice.StartBatchOperationRequest
	s.CommandHarness.Options.AdditionalClientGRPCDialOptions = append(
		s.CommandHarness.Options.AdditionalClientGRPCDialOptions,
		grpc.WithChainUnaryInterceptor(func(
			ctx context.Context,
			method string, req, reply any,
			cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
		) error {
			lastRequestLock.Lock()
			if r, ok := req.(*workflowservice.StartBatchOperationRequest); ok {
				startBatchRequest = r
			}
			lastRequestLock.Unlock()
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
	)

	// Send batch activity unpause
	cmdRes := s.Execute("activity", "unpause",
		"--rps", "1",
		"--address", s.Address(),
		"--query", query,
		"--reason", "unpause-test",
		"--yes",
	)
	s.NoError(cmdRes.Err)
	s.NotEmpty(startBatchRequest.JobId)

	// check activities are running
	checkActivitiesRunning(s, run1)
	checkActivitiesRunning(s, run2)

	// unblock the activities to let them finish
	failActivity.Store(false)
}

func (s *SharedServerSuite) TestResetActivity_BatchSuccess() {
	var failActivity atomic.Bool
	failActivity.Store(true)
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		if failActivity.Load() {
			return nil, fmt.Errorf("update workflow received non-float input")
		}
		return nil, nil
	})

	s.Worker().OnDevWorkflow(func(ctx workflow.Context, a any) (any, error) {
		// override the activity options to allow activity to constantly fail
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:          activityId,
			StartToCloseTimeout: 1 * time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 0,
			},
		})
		var res any
		err := workflow.ExecuteActivity(ctx, DevActivity).Get(ctx, &res)
		return res, err
	})

	run1 := waitWorkflowStarted(s)
	run2 := waitWorkflowStarted(s)

	// Wait for all to appear in list
	query := fmt.Sprintf("WorkflowId = '%s' OR WorkflowId = '%s'", run1.GetID(), run2.GetID())
	s.Eventually(func() bool {
		resp, err := s.Client.ListWorkflow(s.Context, &workflowservice.ListWorkflowExecutionsRequest{
			Query: query,
		})
		s.NoError(err)
		return len(resp.Executions) == 2
	}, 3*time.Second, 100*time.Millisecond)

	// Pause the activities
	res := sendActivityCommand("pause", run1, s, "--activity-id", activityId)
	s.NoError(res.Err)
	res = sendActivityCommand("pause", run2, s, "--activity-id", activityId)
	s.NoError(res.Err)

	// wait for activities to be paused
	checkActivitiesPaused(s, run1)
	checkActivitiesPaused(s, run2)

	var lastRequestLock sync.Mutex
	var startBatchRequest *workflowservice.StartBatchOperationRequest
	s.CommandHarness.Options.AdditionalClientGRPCDialOptions = append(
		s.CommandHarness.Options.AdditionalClientGRPCDialOptions,
		grpc.WithChainUnaryInterceptor(func(
			ctx context.Context,
			method string, req, reply any,
			cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
		) error {
			lastRequestLock.Lock()
			if r, ok := req.(*workflowservice.StartBatchOperationRequest); ok {
				startBatchRequest = r
			}
			lastRequestLock.Unlock()
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
	)

	// Send reset activity unpause
	cmdRes := s.Execute("activity", "reset",
		"--rps", "1",
		"--address", s.Address(),
		"--query", query,
		"--reason", "unpause-test",
		"--yes",
	)
	s.NoError(cmdRes.Err)
	s.NotEmpty(startBatchRequest.JobId)

	// check activities are running
	checkActivitiesRunning(s, run1)
	checkActivitiesRunning(s, run2)

	// unblock the activities to let them finish
	failActivity.Store(false)
}

func (s *SharedServerSuite) TestActivity_Start() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		return "start-result", nil
	})

	res := s.Execute(
		"activity", "start",
		"--activity-id", "start-test",
		"--type", "DevActivity",
		"--task-queue", s.Worker().Options.TaskQueue,
		"--start-to-close-timeout", "30s",
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	out := res.Stdout.String()
	s.Contains(out, "Running execution:")
	s.ContainsOnSameLine(out, "ActivityId", "start-test")
	s.Contains(out, "RunId")
	s.ContainsOnSameLine(out, "Namespace", "default")

	// JSON
	res = s.Execute(
		"activity", "start",
		"-o", "json",
		"--activity-id", "start-test-json",
		"--type", "DevActivity",
		"--task-queue", s.Worker().Options.TaskQueue,
		"--start-to-close-timeout", "30s",
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	var jsonOut map[string]any
	s.NoError(json.Unmarshal(res.Stdout.Bytes(), &jsonOut))
	s.Equal("start-test-json", jsonOut["activityId"])
	s.NotEmpty(jsonOut["runId"])
	s.Equal("default", jsonOut["namespace"])
}

func (s *SharedServerSuite) TestActivity_Start_With_Headers() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		return nil, nil
	})

	var capturedHeader *workflowservice.StartActivityExecutionRequest
	var mu sync.Mutex
	s.CommandHarness.Options.AdditionalClientGRPCDialOptions = append(
		s.CommandHarness.Options.AdditionalClientGRPCDialOptions,
		grpc.WithChainUnaryInterceptor(func(
			ctx context.Context,
			method string, req, reply any,
			cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
		) error {
			if startReq, ok := req.(*workflowservice.StartActivityExecutionRequest); ok {
				mu.Lock()
				capturedHeader = startReq
				mu.Unlock()
			}
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
	)

	res := s.Execute(
		"activity", "start",
		"--activity-id", "header-test",
		"--type", "DevActivity",
		"--task-queue", s.Worker().Options.TaskQueue,
		"--start-to-close-timeout", "30s",
		"--headers", "id=123",
		"--address", s.Address(),
	)
	s.NoError(res.Err)

	mu.Lock()
	defer mu.Unlock()
	s.NotNil(capturedHeader)
	payload := capturedHeader.Header.Fields["id"]
	s.NotNil(payload)
	var val int
	s.NoError(converter.GetDefaultDataConverter().FromPayload(payload, &val))
	s.Equal(123, val)
}

func (s *SharedServerSuite) TestActivity_Execute_Success() {
	var receivedInput any
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		receivedInput = a
		return map[string]string{"foo": "bar"}, nil
	})

	// Text
	res := s.Execute(
		"activity", "execute",
		"--activity-id", "exec-test",
		"--type", "DevActivity",
		"--task-queue", s.Worker().Options.TaskQueue,
		"--start-to-close-timeout", "30s",
		"-i", `"my-input"`,
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	out := res.Stdout.String()
	s.Contains(out, "Running execution:")
	s.ContainsOnSameLine(out, "ActivityId", "exec-test")
	s.Contains(out, "Results:")
	s.ContainsOnSameLine(out, "Status", "COMPLETED")
	s.ContainsOnSameLine(out, "Result", `{"foo":"bar"}`)
	s.Equal("my-input", receivedInput)

	// JSON
	res = s.Execute(
		"activity", "execute",
		"-o", "json",
		"--activity-id", "exec-json-test",
		"--type", "DevActivity",
		"--task-queue", s.Worker().Options.TaskQueue,
		"--start-to-close-timeout", "30s",
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	var jsonOut map[string]any
	s.NoError(json.Unmarshal(res.Stdout.Bytes(), &jsonOut))
	s.Equal("exec-json-test", jsonOut["activityId"])
	s.NotEmpty(jsonOut["runId"])
	s.Equal("COMPLETED", jsonOut["status"])
	s.Equal(map[string]any{"foo": "bar"}, jsonOut["result"])
}

func (s *SharedServerSuite) TestActivity_Execute_Failure() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		return nil, fmt.Errorf("intentional failure")
	})

	// Text
	res := s.Execute(
		"activity", "execute",
		"--activity-id", "exec-fail-test",
		"--type", "DevActivity",
		"--task-queue", s.Worker().Options.TaskQueue,
		"--start-to-close-timeout", "30s",
		"--retry-maximum-attempts", "1",
		"--address", s.Address(),
	)
	s.ErrorContains(res.Err, "activity failed")
	out := res.Stdout.String()
	s.Contains(out, "Running execution:")
	s.Contains(out, "Results:")
	s.Contains(out, "FAILED")
	s.Contains(out, "intentional failure")

	// JSON
	res = s.Execute(
		"activity", "execute",
		"-o", "json",
		"--activity-id", "exec-fail-json-test",
		"--type", "DevActivity",
		"--task-queue", s.Worker().Options.TaskQueue,
		"--start-to-close-timeout", "30s",
		"--retry-maximum-attempts", "1",
		"--address", s.Address(),
	)
	s.Error(res.Err)
	var jsonOut map[string]any
	s.NoError(json.Unmarshal(res.Stdout.Bytes(), &jsonOut))
	s.Equal("exec-fail-json-test", jsonOut["activityId"])
	s.NotEmpty(jsonOut["runId"])
	s.Equal("FAILED", jsonOut["status"])
	failureObj, ok := jsonOut["failure"].(map[string]any)
	s.True(ok, "failure should be a structured object, got: %T", jsonOut["failure"])
	s.Contains(failureObj["message"], "intentional failure")
	s.NotNil(failureObj["applicationFailureInfo"])
}

func (s *SharedServerSuite) TestActivity_Execute_NoJsonShorthandPayloads() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		return map[string]string{"key": "val"}, nil
	})

	// With shorthand (default): result is decoded
	res := s.Execute(
		"activity", "execute",
		"-o", "json",
		"--activity-id", "shorthand-test",
		"--type", "DevActivity",
		"--task-queue", s.Worker().Options.TaskQueue,
		"--start-to-close-timeout", "30s",
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	var jsonOut map[string]any
	s.NoError(json.Unmarshal(res.Stdout.Bytes(), &jsonOut))
	s.Equal(map[string]any{"key": "val"}, jsonOut["result"])

	// Without shorthand: result should be raw payloads with metadata/data
	res = s.Execute(
		"activity", "execute",
		"-o", "json",
		"--no-json-shorthand-payloads",
		"--activity-id", "no-shorthand-test",
		"--type", "DevActivity",
		"--task-queue", s.Worker().Options.TaskQueue,
		"--start-to-close-timeout", "30s",
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	s.NoError(json.Unmarshal(res.Stdout.Bytes(), &jsonOut))
	resultMap, ok := jsonOut["result"].(map[string]any)
	s.True(ok, "result should be a payloads object, got: %T", jsonOut["result"])
	payloads, ok := resultMap["payloads"].([]any)
	s.True(ok, "result should have payloads array")
	s.Len(payloads, 1)
	payload := payloads[0].(map[string]any)
	s.NotNil(payload["metadata"])
	s.NotNil(payload["data"])
}

func (s *SharedServerSuite) TestActivity_Execute_RetriesOnEmptyPollResponse() {
	// Activity sleeps longer than the server's activity.longPollTimeout (2s),
	// forcing at least one empty poll response before the result arrives.
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		time.Sleep(3 * time.Second)
		return "standalone-result", nil
	})

	res := s.Execute(
		"activity", "execute",
		"--activity-id", "poll-retry-test",
		"--type", "DevActivity",
		"--task-queue", s.Worker().Options.TaskQueue,
		"--start-to-close-timeout", "30s",
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	s.Contains(res.Stdout.String(), "standalone-result")
}

// startActivity starts an activity via the CLI and returns
// the parsed JSON response containing activityId and runId.
func (s *SharedServerSuite) startActivity(activityID string, extraArgs ...string) map[string]any {
	args := []string{
		"activity", "start",
		"-o", "json",
		"--activity-id", activityID,
		"--type", "DevActivity",
		"--task-queue", s.Worker().Options.TaskQueue,
		"--start-to-close-timeout", "30s",
		"--address", s.Address(),
	}
	args = append(args, extraArgs...)
	res := s.Execute(args...)
	s.NoError(res.Err)
	var jsonOut map[string]any
	s.NoError(json.Unmarshal(res.Stdout.Bytes(), &jsonOut))
	return jsonOut
}

func (s *SharedServerSuite) TestActivity_Result() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		return "result-value", nil
	})

	started := s.startActivity("result-test")

	res := s.Execute(
		"activity", "result",
		"--activity-id", "result-test",
		"--run-id", started["runId"].(string),
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	s.Contains(res.Stdout.String(), "result-value")

	// JSON output without --run-id
	res = s.Execute(
		"activity", "result",
		"-o", "json",
		"--activity-id", "result-test",
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	var jsonOut map[string]any
	s.NoError(json.Unmarshal(res.Stdout.Bytes(), &jsonOut))
	s.Equal("COMPLETED", jsonOut["status"])
	s.Equal("result-test", jsonOut["activityId"])
	s.Equal("result-value", jsonOut["result"])
}

func (s *SharedServerSuite) TestActivity_Result_NotFound() {
	res := s.Execute(
		"activity", "result",
		"--activity-id", "nonexistent-activity-id",
		"--address", s.Address(),
	)
	s.Error(res.Err)
	s.Contains(res.Err.Error(), "not found")
	s.NotContains(res.Stdout.String(), "FAILED")
}

func (s *SharedServerSuite) TestActivity_Describe() {
	activityStarted := make(chan struct{})
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		close(activityStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	})

	started := s.startActivity("describe-test",
		"--schedule-to-close-timeout", "300s",
		"--schedule-to-start-timeout", "60s",
		"--heartbeat-timeout", "15s",
		"--retry-maximum-attempts", "5",
		"--retry-initial-interval", "2s",
		"--retry-backoff-coefficient", "3",
		"--retry-maximum-interval", "120s",
	)
	runID := started["runId"].(string)
	<-activityStarted

	// Text
	res := s.Execute(
		"activity", "describe",
		"--activity-id", "describe-test",
		"--run-id", runID,
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	out := res.Stdout.String()
	s.ContainsOnSameLine(out, "ActivityId", "describe-test")
	s.ContainsOnSameLine(out, "Type", "DevActivity")
	s.ContainsOnSameLine(out, "Status", "Running")
	s.ContainsOnSameLine(out, "TaskQueue", s.Worker().Options.TaskQueue)
	s.ContainsOnSameLine(out, "StartToCloseTimeout", "30s")
	s.ContainsOnSameLine(out, "ScheduleToCloseTimeout", "5m0s")
	s.ContainsOnSameLine(out, "ScheduleToStartTimeout", "1m0s")
	s.ContainsOnSameLine(out, "HeartbeatTimeout", "15s")
	s.ContainsOnSameLine(out, "Attempt", "1")
	s.Contains(out, "LastWorkerIdentity")
	s.NotContains(out, `{"name":`)

	// JSON
	res = s.Execute(
		"activity", "describe",
		"-o", "json",
		"--activity-id", "describe-test",
		"--run-id", runID,
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	var jsonOut map[string]any
	s.NoError(json.Unmarshal(res.Stdout.Bytes(), &jsonOut))
	s.Equal("describe-test", jsonOut["activityId"])
	s.NotNil(jsonOut["activityType"])
	s.NotNil(jsonOut["taskQueue"])
	s.Equal("300s", jsonOut["scheduleToCloseTimeout"])
	s.Equal("60s", jsonOut["scheduleToStartTimeout"])
	s.Equal("30s", jsonOut["startToCloseTimeout"])
	s.Equal("15s", jsonOut["heartbeatTimeout"])
	retryPolicy, ok := jsonOut["retryPolicy"].(map[string]any)
	s.True(ok, "retryPolicy should be present in JSON describe")
	s.Equal(float64(5), retryPolicy["maximumAttempts"])
	s.Equal("2s", retryPolicy["initialInterval"])
	s.Equal(float64(3), retryPolicy["backoffCoefficient"])
	s.Equal("120s", retryPolicy["maximumInterval"])

	// Raw: should contain proto JSON format
	res = s.Execute(
		"activity", "describe",
		"--raw",
		"--activity-id", "describe-test",
		"--run-id", runID,
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	rawOut := res.Stdout.String()
	s.Contains(rawOut, "describe-test")
	s.Contains(rawOut, `{"name":"DevActivity"}`)
}

// Text-only: verifies LastFailure is rendered as text not JSON.
func (s *SharedServerSuite) TestActivity_Describe_FailedLastFailure() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		return nil, fmt.Errorf("describe-failure-msg")
	})

	started := s.startActivity("describe-fail-test", "--retry-maximum-attempts", "1")

	// Wait for the activity to fail
	handle := s.Client.GetActivityHandle(client.GetActivityHandleOptions{
		ActivityID: "describe-fail-test",
		RunID:      started["runId"].(string),
	})
	_ = handle.Get(s.Context, nil)

	res := s.Execute(
		"activity", "describe",
		"--activity-id", "describe-fail-test",
		"--run-id", started["runId"].(string),
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	out := res.Stdout.String()
	// LastFailure should be human-readable, not raw JSON
	s.Contains(out, "describe-failure-msg")
	s.NotContains(out, `"message":"describe-failure-msg"`)
}

func (s *SharedServerSuite) TestActivity_List() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		return "listed", nil
	})

	s.startActivity("list-test-1")
	s.startActivity("list-test-2")
	s.startActivity("list-test-3")

	// Wait for all three to be visible
	s.Eventually(func() bool {
		res := s.Execute(
			"activity", "list",
			"--address", s.Address(),
		)
		out := res.Stdout.String()
		return res.Err == nil &&
			strings.Contains(out, "list-test-1") &&
			strings.Contains(out, "list-test-2") &&
			strings.Contains(out, "list-test-3")
	}, 5*time.Second, 200*time.Millisecond)

	// --limit should cap the number of results
	res := s.Execute(
		"activity", "list",
		"--limit", "2",
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	lines := strings.Split(strings.TrimSpace(res.Stdout.String()), "\n")
	s.Equal(3, len(lines), "expected header + 2 rows with --limit 2, got: %s", res.Stdout.String())

	// JSON
	res = s.Execute(
		"activity", "list",
		"-o", "json",
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	out := res.Stdout.String()
	s.ContainsOnSameLine(out, "activityId", "list-test-1")
	s.ContainsOnSameLine(out, "status", "ACTIVITY_EXECUTION_STATUS_COMPLETED")

	// JSONL
	res = s.Execute(
		"activity", "list",
		"-o", "jsonl",
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	jsonlLines := strings.Split(strings.TrimSpace(res.Stdout.String()), "\n")
	s.GreaterOrEqual(len(jsonlLines), 3)
	seen := map[string]bool{}
	for _, line := range jsonlLines {
		var exec struct {
			ActivityId string `json:"activityId"`
		}
		s.NoError(json.Unmarshal([]byte(line), &exec))
		seen[exec.ActivityId] = true
	}
	s.True(seen["list-test-1"])
	s.True(seen["list-test-2"])
	s.True(seen["list-test-3"])
}

func (s *SharedServerSuite) TestActivity_Count() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		return "counted", nil
	})

	s.startActivity("count-test")

	// Text
	s.Eventually(func() bool {
		res := s.Execute(
			"activity", "count",
			"--address", s.Address(),
		)
		return res.Err == nil && strings.Contains(res.Stdout.String(), "Total:")
	}, 5*time.Second, 200*time.Millisecond)

	// Grouped text
	s.Eventually(func() bool {
		res := s.Execute(
			"activity", "count",
			"--address", s.Address(),
			"--query", "GROUP BY ExecutionStatus",
		)
		if res.Err != nil {
			return false
		}
		out := res.Stdout.String()
		return strings.Contains(out, "Total:") && strings.Contains(out, "Group total:")
	}, 5*time.Second, 200*time.Millisecond)

	// JSON
	res := s.Execute(
		"activity", "count",
		"--address", s.Address(),
		"-o", "json",
	)
	s.NoError(res.Err)
	var jsonOut map[string]any
	s.NoError(json.Unmarshal(res.Stdout.Bytes(), &jsonOut))
	_, ok := jsonOut["count"]
	s.True(ok)
}

// No JSON variant: command produces no output on success in any mode.
func (s *SharedServerSuite) TestActivity_Complete_ByRunId() {
	activityStarted := make(chan struct{})
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		close(activityStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	})

	started := s.startActivity("sa-complete-test")
	runID := started["runId"].(string)
	<-activityStarted

	res := s.Execute(
		"activity", "complete",
		"--activity-id", "sa-complete-test",
		"--run-id", runID,
		"--result", `"completed-externally"`,
		"--identity", identity,
		"--address", s.Address(),
	)
	s.NoError(res.Err)

	handle := s.Client.GetActivityHandle(client.GetActivityHandleOptions{
		ActivityID: "sa-complete-test",
		RunID:      runID,
	})
	var actual string
	s.NoError(handle.Get(s.Context, &actual))
	s.Equal("completed-externally", actual)
}

// No JSON variant: command produces no output on success in any mode.
func (s *SharedServerSuite) TestActivity_Fail_ByRunId() {
	activityStarted := make(chan struct{})
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		close(activityStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	})

	started := s.startActivity("sa-fail-test")
	runID := started["runId"].(string)
	<-activityStarted

	res := s.Execute(
		"activity", "fail",
		"--activity-id", "sa-fail-test",
		"--run-id", runID,
		"--reason", "external-failure",
		"--identity", identity,
		"--address", s.Address(),
	)
	s.NoError(res.Err)

	handle := s.Client.GetActivityHandle(client.GetActivityHandleOptions{
		ActivityID: "sa-fail-test",
		RunID:      runID,
	})
	err := handle.Get(s.Context, nil)
	s.Error(err)
	s.Contains(err.Error(), "external-failure")
}

// No JSON variant: Println outputs the same text regardless of -o json (matches workflow cancel).
func (s *SharedServerSuite) TestActivity_Cancel() {
	activityStarted := make(chan struct{})
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		close(activityStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	})

	started := s.startActivity("cancel-test")
	runID := started["runId"].(string)
	<-activityStarted

	res := s.Execute(
		"activity", "cancel",
		"--activity-id", "cancel-test",
		"--run-id", runID,
		"--reason", "test-cancel",
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	s.Contains(res.Stdout.String(), "Cancellation requested")

	handle := s.Client.GetActivityHandle(client.GetActivityHandleOptions{
		ActivityID: "cancel-test",
		RunID:      runID,
	})
	s.Eventually(func() bool {
		desc, err := handle.Describe(s.Context, client.DescribeActivityOptions{})
		return err == nil && desc.RunState.String() == "CancelRequested"
	}, 5*time.Second, 100*time.Millisecond)
}

// No JSON variant: Println outputs the same text regardless of -o json (matches workflow terminate).
func (s *SharedServerSuite) TestActivity_Terminate() {
	activityStarted := make(chan struct{})
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		close(activityStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	})

	started := s.startActivity("terminate-test")
	runID := started["runId"].(string)
	<-activityStarted

	res := s.Execute(
		"activity", "terminate",
		"--activity-id", "terminate-test",
		"--run-id", runID,
		"--reason", "test-terminate",
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	s.Contains(res.Stdout.String(), "Activity terminated")

	handle := s.Client.GetActivityHandle(client.GetActivityHandleOptions{
		ActivityID: "terminate-test",
		RunID:      runID,
	})
	err := handle.Get(s.Context, nil)
	s.Error(err)
	s.Contains(err.Error(), "terminated")
}

func (s *SharedServerSuite) TestActivity_SearchAttributes() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		return nil, nil
	})

	for _, sa := range []struct{ name, typ string }{
		{"SATestBool", "Bool"},
		{"SATestInt", "Int"},
		{"SATestDouble", "Double"},
		{"SATestKeyword", "Keyword"},
		{"SATestText", "Text"},
		{"SATestKeywordList", "KeywordList"},
	} {
		res := s.Execute(
			"operator", "search-attribute", "create",
			"--address", s.Address(),
			"--name", sa.name,
			"--type", sa.typ,
		)
		s.NoError(res.Err)
	}

	unique := uuid.NewString()[:8]

	// Bool (JSON bool → NewSearchAttributeKeyBool)
	s.startActivity("sa-bool-"+unique,
		"--search-attribute", `SATestBool=true`,
	)
	s.Eventually(func() bool {
		res := s.Execute("activity", "list", "--address", s.Address(),
			"--query", `SATestBool = true`)
		return res.Err == nil && strings.Contains(res.Stdout.String(), "sa-bool-"+unique)
	}, 5*time.Second, 200*time.Millisecond)

	// Int (JSON number → float64 → sent as Float64; server decodes via schema)
	s.startActivity("sa-int-"+unique,
		"--search-attribute", `SATestInt=42`,
	)
	s.Eventually(func() bool {
		res := s.Execute("activity", "list", "--address", s.Address(),
			"--query", `SATestInt = 42`)
		return res.Err == nil && strings.Contains(res.Stdout.String(), "sa-int-"+unique)
	}, 5*time.Second, 200*time.Millisecond)

	// Double (JSON number → float64 → NewSearchAttributeKeyFloat64)
	s.startActivity("sa-double-"+unique,
		"--search-attribute", `SATestDouble=3.14`,
	)
	s.Eventually(func() bool {
		res := s.Execute("activity", "list", "--address", s.Address(),
			"--query", `SATestDouble = 3.14`)
		return res.Err == nil && strings.Contains(res.Stdout.String(), "sa-double-"+unique)
	}, 5*time.Second, 200*time.Millisecond)

	// Keyword (JSON string → NewSearchAttributeKeyKeyword)
	s.startActivity("sa-keyword-"+unique,
		"--search-attribute", fmt.Sprintf(`SATestKeyword="kw-%s"`, unique),
	)
	s.Eventually(func() bool {
		res := s.Execute("activity", "list", "--address", s.Address(),
			"--query", fmt.Sprintf(`SATestKeyword = "kw-%s"`, unique))
		return res.Err == nil && strings.Contains(res.Stdout.String(), "sa-keyword-"+unique)
	}, 5*time.Second, 200*time.Millisecond)

	// Text (JSON string → sent as Keyword; server decodes via schema)
	s.startActivity("sa-text-"+unique,
		"--search-attribute", fmt.Sprintf(`SATestText="text value %s"`, unique),
	)
	s.Eventually(func() bool {
		res := s.Execute("activity", "list", "--address", s.Address(),
			"--query", fmt.Sprintf(`SATestText = "text value %s"`, unique))
		return res.Err == nil && strings.Contains(res.Stdout.String(), "sa-text-"+unique)
	}, 5*time.Second, 200*time.Millisecond)

	// KeywordList (JSON array → []any → NewSearchAttributeKeyKeywordList)
	s.startActivity("sa-kwlist-"+unique,
		"--search-attribute", `SATestKeywordList=["alpha","beta"]`,
	)
	s.Eventually(func() bool {
		res := s.Execute("activity", "list", "--address", s.Address(),
			"--query", `SATestKeywordList = "alpha"`)
		return res.Err == nil && strings.Contains(res.Stdout.String(), "sa-kwlist-"+unique)
	}, 5*time.Second, 200*time.Millisecond)
}

func (s *SharedServerSuite) TestActivity_SearchAttributes_Datetime() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		return nil, nil
	})

	res := s.Execute(
		"operator", "search-attribute", "create",
		"--address", s.Address(),
		"--name", "SATestDatetime",
		"--type", "Datetime",
	)
	s.NoError(res.Err)

	s.startActivity("sa-datetime-test",
		"--search-attribute", `SATestDatetime="2024-01-15T00:00:00Z"`,
	)
	s.Eventually(func() bool {
		res = s.Execute(
			"activity", "list",
			"--address", s.Address(),
			"--query", `SATestDatetime > "2024-01-14T00:00:00Z"`,
		)
		return res.Err == nil && strings.Contains(res.Stdout.String(), "sa-datetime-test")
	}, 5*time.Second, 200*time.Millisecond)
}

func (s *SharedServerSuite) TestActivity_SearchAttributes_InvalidKeywordList() {
	res := s.Execute(
		"activity", "start",
		"--activity-id", "sa-invalid-kwlist",
		"--type", "DevActivity",
		"--task-queue", s.Worker().Options.TaskQueue,
		"--start-to-close-timeout", "30s",
		"--search-attribute", `Foo=[1,"a"]`,
		"--address", s.Address(),
	)
	s.Error(res.Err)
	s.Contains(res.Err.Error(), "array element 0 is float64, not string")
}

func (s *SharedServerSuite) TestActivity_List_Pagination() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		return "paginated", nil
	})

	uniqueKW := "page-" + uuid.NewString()[:8]
	for i := 0; i < 5; i++ {
		s.startActivity(fmt.Sprintf("page-test-%d", i),
			"--search-attribute", fmt.Sprintf(`CustomKeywordField="%s"`, uniqueKW),
		)
	}

	// Wait for all 5 to be visible
	s.Eventually(func() bool {
		res := s.Execute(
			"activity", "list",
			"--address", s.Address(),
			"--query", fmt.Sprintf(`CustomKeywordField = "%s"`, uniqueKW),
		)
		return res.Err == nil && strings.Count(res.Stdout.String(), "page-test-") >= 5
	}, 5*time.Second, 200*time.Millisecond)

	// Small page size forces multi-page fetching; verify all 5 appear
	res := s.Execute(
		"activity", "list",
		"--page-size", "2",
		"--address", s.Address(),
		"--query", fmt.Sprintf(`CustomKeywordField = "%s"`, uniqueKW),
	)
	s.NoError(res.Err)
	s.Equal(5, strings.Count(res.Stdout.String(), "page-test-"))

	// --limit 3 with page-size 2 should return exactly 3
	res = s.Execute(
		"activity", "list",
		"--page-size", "2",
		"--limit", "3",
		"--address", s.Address(),
		"--query", fmt.Sprintf(`CustomKeywordField = "%s"`, uniqueKW),
	)
	s.NoError(res.Err)
	s.Equal(3, strings.Count(res.Stdout.String(), "page-test-"))
}

// =============================================================================
// Standalone Activity (SAA) UX bug coverage
//
// The following tests cover bugs and rough edges surfaced while exercising the
// `temporal activity` subcommands against Standalone Activities during the
// public-preview UX pass. Each test is written to fail today and to pass once
// the underlying CLI (or, where noted, server) bug is fixed. Update or remove
// individual tests as the corresponding fixes land.
// =============================================================================

// Bug: `temporal activity describe` (default text output) hides recorded
// heartbeat info. The fields are present on the underlying ActivityExecutionInfo
// proto and surfaced via `-o json`, but the text card omits them entirely,
// leaving SAA operators with no visible signal that their workers are
// heartbeating. The information is critical for diagnosing stuck activities.
//
// Expected after fix: text describe of an SAA that has heartbeated should
// include LastHeartbeatTime, TotalHeartbeatCount, and HeartbeatDetails.
func (s *SharedServerSuite) TestActivity_Describe_TextShowsHeartbeatInfo() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		// Record an immediate heartbeat with non-trivial details, then keep
		// beating until canceled so describe can be inspected mid-flight.
		activity.RecordHeartbeat(ctx, map[string]any{"step": "first", "n": 1})
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		i := 1
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ticker.C:
				i++
				activity.RecordHeartbeat(ctx, map[string]any{"step": "loop", "n": i})
			}
		}
	})

	started := s.startActivity("hb-describe-test", "--heartbeat-timeout", "10s")
	runID := started["runId"].(string)

	// Wait for the server to record at least one heartbeat.
	handle := s.Client.GetActivityHandle(client.GetActivityHandleOptions{
		ActivityID: "hb-describe-test",
		RunID:      runID,
	})
	s.Eventually(func() bool {
		desc, err := handle.Describe(s.Context, client.DescribeActivityOptions{})
		return err == nil && !desc.LastHeartbeatTime.IsZero()
	}, 5*time.Second, 100*time.Millisecond)

	res := s.Execute(
		"activity", "describe",
		"--activity-id", "hb-describe-test",
		"--run-id", runID,
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	out := res.Stdout.String()
	// Bug: today the text card prints none of these. After fix, all three
	// should appear in the default text output.
	s.Contains(out, "LastHeartbeatTime",
		"default-text describe should expose LastHeartbeatTime; today the field is in -o json only")
	s.Contains(out, "TotalHeartbeatCount",
		"default-text describe should expose TotalHeartbeatCount")
	// Heartbeat details is the most-asked-for field — heartbeating activities
	// rely on it for checkpoint/progress reporting that operators need to see.
	s.Contains(out, "HeartbeatDetails",
		"default-text describe should expose HeartbeatDetails")
}

// Bug: text describe omits the user-supplied static summary and details that
// SAA users explicitly attach for human consumption. They are stored in
// ActivityExecutionInfo.user_metadata and visible via `-o json` and the Web
// UI, but the default text card drops them.
//
// Expected after fix: `--static-summary` and `--static-details` values supplied
// at start time should appear (decoded) in default text describe output.
func (s *SharedServerSuite) TestActivity_Describe_TextShowsUserMetadata() {
	activityStarted := make(chan struct{})
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		close(activityStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	})

	started := s.startActivity("um-describe-test",
		"--static-summary", "Backfill batch #42",
		"--static-details", "Reprocesses orders for tenant=acme",
	)
	runID := started["runId"].(string)
	<-activityStarted

	res := s.Execute(
		"activity", "describe",
		"--activity-id", "um-describe-test",
		"--run-id", runID,
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	out := res.Stdout.String()
	// Bug: text describe prints nothing user-metadata-related.
	s.Contains(out, "Backfill batch #42",
		"static-summary should be visible in default text describe output")
	s.Contains(out, "Reprocesses orders for tenant=acme",
		"static-details should be visible in default text describe output")
}

// Bug: text describe omits CanceledReason. When an operator cancels an SAA
// with `--reason "..."`, the supplied reason is recorded on the server (and
// visible via `-o json`) but invisible in the default text card, making it
// impossible to see why an activity was canceled without dropping into JSON.
//
// Expected after fix: the cancellation reason should appear in default text
// describe output once the server has recorded it.
func (s *SharedServerSuite) TestActivity_Describe_TextShowsCanceledReason() {
	activityStarted := make(chan struct{})
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		close(activityStarted)
		// Heartbeat so cancellation propagates back into the activity ctx.
		for {
			activity.RecordHeartbeat(ctx)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
	})

	started := s.startActivity("cr-describe-test", "--heartbeat-timeout", "10s")
	runID := started["runId"].(string)
	<-activityStarted

	cancelRes := s.Execute(
		"activity", "cancel",
		"--activity-id", "cr-describe-test",
		"--run-id", runID,
		"--reason", "operator-initiated-rollback",
		"--address", s.Address(),
	)
	s.NoError(cancelRes.Err)

	// Wait for the server to record the cancellation reason.
	handle := s.Client.GetActivityHandle(client.GetActivityHandleOptions{
		ActivityID: "cr-describe-test",
		RunID:      runID,
	})
	s.Eventually(func() bool {
		desc, err := handle.Describe(s.Context, client.DescribeActivityOptions{})
		return err == nil && desc.CanceledReason == "operator-initiated-rollback"
	}, 5*time.Second, 100*time.Millisecond)

	res := s.Execute(
		"activity", "describe",
		"--activity-id", "cr-describe-test",
		"--run-id", runID,
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	out := res.Stdout.String()
	// Bug: text describe doesn't surface canceledReason at all.
	s.Contains(out, "operator-initiated-rollback",
		"default-text describe should expose CanceledReason after a cancel-with-reason")
}

// Bug: `temporal activity pause` is documented as "Not supported for Standalone
// Activities", but the only error an SAA user sees today is cobra's generic
// `required flag(s) "workflow-id" not set`, suggesting they merely forgot a
// flag rather than that the command is fundamentally inapplicable.
//
// Expected after fix: invoking `pause` for an SAA (i.e. with --activity-id but
// no --workflow-id) should yield a clear "not supported for Standalone
// Activities" error.
func (s *SharedServerSuite) TestActivity_Pause_StandaloneActivity_ClearError() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	s.startActivity("pause-saa-test")

	res := s.Execute(
		"activity", "pause",
		"--activity-id", "pause-saa-test",
		"--address", s.Address(),
	)
	s.Error(res.Err)
	msg := res.Err.Error()
	lower := strings.ToLower(msg)
	// Bug: today the error is `required flag(s) "workflow-id" not set`.
	s.NotContainsf(lower, `required flag(s) "workflow-id"`,
		"pause SAA error should not be cobra's generic required-flag message; got: %s", msg)
	s.Containsf(lower, "standalone",
		"pause SAA error should explicitly mention Standalone Activities; got: %s", msg)
}

// Bug: `temporal activity unpause` is documented as "Not supported for
// Standalone Activities", but an SAA user sees `must set either workflow ID
// or query`, which doesn't tell them the command is inapplicable.
//
// Expected after fix: invoking `unpause` with --activity-id only should yield
// a clear "not supported for Standalone Activities" error.
func (s *SharedServerSuite) TestActivity_Unpause_StandaloneActivity_ClearError() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	s.startActivity("unpause-saa-test")

	res := s.Execute(
		"activity", "unpause",
		"--activity-id", "unpause-saa-test",
		"--address", s.Address(),
	)
	s.Error(res.Err)
	msg := res.Err.Error()
	lower := strings.ToLower(msg)
	s.NotContainsf(lower, "either --activity-id and --workflow-id, or --query must be set",
		"unpause SAA error should not be the generic 'either workflow-id or query' message; got: %s", msg)
	s.Containsf(lower, "standalone",
		"unpause SAA error should explicitly mention Standalone Activities; got: %s", msg)
}

// Bug: `temporal activity reset` is documented as "Not supported for Standalone
// Activities", but an SAA user sees `must set either workflow ID or query`.
//
// Expected after fix: invoking `reset` with --activity-id only should yield a
// clear "not supported for Standalone Activities" error.
func (s *SharedServerSuite) TestActivity_Reset_StandaloneActivity_ClearError() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	s.startActivity("reset-saa-test")

	res := s.Execute(
		"activity", "reset",
		"--activity-id", "reset-saa-test",
		"--address", s.Address(),
	)
	s.Error(res.Err)
	msg := res.Err.Error()
	lower := strings.ToLower(msg)
	s.NotContainsf(lower, "either --activity-id and --workflow-id, or --query must be set",
		"reset SAA error should not be the generic 'either workflow-id or query' message; got: %s", msg)
	s.Containsf(lower, "standalone",
		"reset SAA error should explicitly mention Standalone Activities; got: %s", msg)
}

// Bug: `temporal activity update-options` is documented as "Not supported for
// Standalone Activities", but an SAA user trying to retune timeouts sees the
// generic `must set either workflow ID or query`, again hiding the fact that
// the command is fundamentally inapplicable.
//
// Expected after fix: invoking `update-options` for an SAA should yield a
// clear "not supported for Standalone Activities" error.
func (s *SharedServerSuite) TestActivity_UpdateOptions_StandaloneActivity_ClearError() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	s.startActivity("uo-saa-test")

	res := s.Execute(
		"activity", "update-options",
		"--activity-id", "uo-saa-test",
		"--start-to-close-timeout", "45s",
		"--address", s.Address(),
	)
	s.Error(res.Err)
	msg := res.Err.Error()
	lower := strings.ToLower(msg)
	s.NotContainsf(lower, "either --activity-id and --workflow-id, or --query must be set",
		"update-options SAA error should not be the generic 'either workflow-id or query' message; got: %s", msg)
	s.Containsf(lower, "standalone",
		"update-options SAA error should explicitly mention Standalone Activities; got: %s", msg)
}

// Bug: `temporal activity terminate` (and other commands using defaultReason)
// always renders its default reason as `Requested from CLI by <unknown-user>`
// on every machine, because the condition in commands.workflow.go's username()
// is inverted (`err != nil` instead of `err == nil`), so the success branch of
// `user.Current()` is never taken. This makes the recorded audit trail useless
// for distinguishing operators.
//
// Expected after fix: terminating without `--reason` should record a default
// reason that does not contain the literal "<unknown-user>".
func (s *SharedServerSuite) TestActivity_Terminate_DefaultReason_NoUnknownUser() {
	activityStarted := make(chan struct{})
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		close(activityStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	})

	started := s.startActivity("terminate-default-reason-test")
	runID := started["runId"].(string)
	<-activityStarted

	res := s.Execute(
		"activity", "terminate",
		"--activity-id", "terminate-default-reason-test",
		"--run-id", runID,
		"--address", s.Address(),
	)
	s.NoError(res.Err)

	// Wait for the activity to actually transition to TERMINATED, then poll
	// for its outcome via `activity result -o json` — that command surfaces
	// the recorded failure message, which is where the termination reason
	// (the supplied --reason or defaultReason()) lives.
	handle := s.Client.GetActivityHandle(client.GetActivityHandleOptions{
		ActivityID: "terminate-default-reason-test",
		RunID:      runID,
	})
	s.Eventually(func() bool {
		desc, err := handle.Describe(s.Context, client.DescribeActivityOptions{})
		return err == nil && desc.Status == enums.ACTIVITY_EXECUTION_STATUS_TERMINATED
	}, 5*time.Second, 100*time.Millisecond)

	res = s.Execute(
		"activity", "result", "-o", "json",
		"--activity-id", "terminate-default-reason-test",
		"--run-id", runID,
		"--address", s.Address(),
	)
	// `result` returns a non-nil err for any non-success outcome; the JSON
	// payload (with the failure body) is still printed on stdout.
	s.NotEmptyf(res.Stdout.String(), "activity result should produce output for terminated SAA")
	var outcome map[string]any
	s.NoError(json.Unmarshal(res.Stdout.Bytes(), &outcome))
	failure, _ := outcome["failure"].(map[string]any)
	s.NotNilf(failure, "activity result -o json for a terminated SAA should include the failure body; got: %s", res.Stdout.String())
	failureMsg, _ := failure["message"].(string)
	s.Containsf(failureMsg, "Requested from CLI",
		"recorded reason should be the CLI-default 'Requested from CLI by <user>' message; got: %q", failureMsg)
	// Bug: defaultReason() always renders as "Requested from CLI by <unknown-user>"
	// because of the inverted `err != nil` check in username() in
	// commands.workflow.go: `if u, err := user.Current(); err != nil && u.Username != ""`
	// means the success branch of user.Current() is never taken.
	s.NotContainsf(failureMsg, "<unknown-user>",
		"default termination reason should not literally contain '<unknown-user>'; "+
			"the inverted condition in username() makes the user.Current() success path unreachable; got: %q",
		failureMsg)
}

// Bug: `temporal activity result` reports `Status: FAILED` when the activity
// outcome is a cancellation. printActivityFailure unconditionally renders
// FAILED for any non-Result outcome, conflating CANCELED with FAILED. SAA
// users polling for the outcome of a cancellation see the same status they
// would see for a real worker-side failure.
//
// Expected after fix: `result` on a CANCELED SAA should report Status=CANCELED
// in both text and JSON output.
func (s *SharedServerSuite) TestActivity_Result_OnCanceledActivity_ShowsCanceledStatus() {
	activityStarted := make(chan struct{})
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		close(activityStarted)
		// Heartbeat in a tight loop so the cancellation request propagates
		// promptly into the activity ctx.
		for {
			activity.RecordHeartbeat(ctx)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
	})

	started := s.startActivity("result-canceled-test", "--heartbeat-timeout", "10s")
	runID := started["runId"].(string)
	<-activityStarted

	cancelRes := s.Execute(
		"activity", "cancel",
		"--activity-id", "result-canceled-test",
		"--run-id", runID,
		"--reason", "test-cancel",
		"--address", s.Address(),
	)
	s.NoError(cancelRes.Err)

	// Wait for the activity to actually transition to CANCELED on the server.
	handle := s.Client.GetActivityHandle(client.GetActivityHandleOptions{
		ActivityID: "result-canceled-test",
		RunID:      runID,
	})
	s.Eventually(func() bool {
		desc, err := handle.Describe(s.Context, client.DescribeActivityOptions{})
		return err == nil && desc.Status == enums.ACTIVITY_EXECUTION_STATUS_CANCELED
	}, 10*time.Second, 100*time.Millisecond)

	// Text output: should report Status: CANCELED, not FAILED.
	res := s.Execute(
		"activity", "result",
		"--activity-id", "result-canceled-test",
		"--run-id", runID,
		"--address", s.Address(),
	)
	out := res.Stdout.String()
	// Bug: printActivityFailure prints Status FAILED for any non-Result outcome.
	s.ContainsOnSameLine(out, "Status", "CANCELED")

	// JSON output: status field should be "CANCELED".
	res = s.Execute(
		"activity", "result", "-o", "json",
		"--activity-id", "result-canceled-test",
		"--run-id", runID,
		"--address", s.Address(),
	)
	var jsonOut map[string]any
	s.NoError(json.Unmarshal(res.Stdout.Bytes(), &jsonOut))
	s.Equal("CANCELED", jsonOut["status"],
		"`activity result -o json` should report status=CANCELED for a canceled SAA, not FAILED")
}

// Bug: the default-text columns of `temporal activity list` are unhelpfully
// sparse: only Status, ActivityId, Type, StartTime are shown, even though the
// server includes TaskQueue, CloseTime, and ExecutionDuration in
// ActivityExecutionListInfo. SAA operators have to drop to `-o json` (or call
// describe per row) to see which task queue an activity is on or when it
// finished — both common questions during triage.
//
// Expected after fix: the text-mode list should include at least TaskQueue
// and CloseTime as columns.
func (s *SharedServerSuite) TestActivity_List_TextIncludesTaskQueueAndCloseTime() {
	s.Worker().OnDevActivity(func(ctx context.Context, a any) (any, error) {
		return "done", nil
	})

	uniqueKW := "list-cols-" + uuid.NewString()[:8]
	activityID := "list-cols-test-" + uniqueKW
	s.startActivity(activityID,
		"--search-attribute", fmt.Sprintf(`CustomKeywordField="%s"`, uniqueKW),
	)

	// Wait for the row to be visible (and terminal — CloseTime only populates
	// after the activity reaches a terminal state).
	s.Eventually(func() bool {
		r := s.Execute(
			"activity", "list", "-o", "json",
			"--query", fmt.Sprintf(`CustomKeywordField = "%s"`, uniqueKW),
			"--address", s.Address(),
		)
		return r.Err == nil && strings.Contains(r.Stdout.String(), "ACTIVITY_EXECUTION_STATUS_COMPLETED")
	}, 5*time.Second, 200*time.Millisecond)

	res := s.Execute(
		"activity", "list",
		"--query", fmt.Sprintf(`CustomKeywordField = "%s"`, uniqueKW),
		"--address", s.Address(),
	)
	s.NoError(res.Err)
	out := res.Stdout.String()
	// Bug: text list omits both columns even though the server returns them.
	s.Contains(out, "TaskQueue",
		"`activity list` text output should include the TaskQueue column")
	s.Contains(out, "CloseTime",
		"`activity list` text output should include the CloseTime column for terminal activities")
}

// Bug: `temporal activity list` shows Status=Running for SAAs that have been
// scheduled but never picked up by a worker (e.g. wrong task-queue name or the
// worker is offline). ActivityExecutionStatus.RUNNING covers both SCHEDULED
// and STARTED PendingActivityState values, so SAA operators have no way to
// tell from the list view whether their activity is actually progressing or
// stuck waiting for a worker. The fix may be server-side (split the status,
// or expose run_state in the list payload) or CLI-side (cross-reference with
// describe), but either way the row for an unstarted SAA must not display
// "Running".
//
// Expected after fix: an SAA scheduled on a task queue with no worker should
// not appear with Status=Running in `activity list` text output.
func (s *SharedServerSuite) TestActivity_List_TextStatusForUnstartedSAA() {
	uniqueKW := "no-worker-" + uuid.NewString()[:8]
	activityID := "no-worker-test-" + uniqueKW
	noWorkerTQ := "no-worker-tq-" + uuid.NewString()[:8]

	res := s.Execute(
		"activity", "start",
		"--activity-id", activityID,
		"--type", "DevActivity",
		"--task-queue", noWorkerTQ,
		"--start-to-close-timeout", "300s",
		"--schedule-to-close-timeout", "600s",
		"--search-attribute", fmt.Sprintf(`CustomKeywordField="%s"`, uniqueKW),
		"--address", s.Address(),
	)
	s.NoError(res.Err)

	s.Eventually(func() bool {
		r := s.Execute(
			"activity", "list",
			"--query", fmt.Sprintf(`CustomKeywordField = "%s"`, uniqueKW),
			"--address", s.Address(),
		)
		return r.Err == nil && strings.Contains(r.Stdout.String(), activityID)
	}, 5*time.Second, 200*time.Millisecond)

	res = s.Execute(
		"activity", "list",
		"--query", fmt.Sprintf(`CustomKeywordField = "%s"`, uniqueKW),
		"--address", s.Address(),
	)
	s.NoError(res.Err)

	var ourLine string
	for _, line := range strings.Split(res.Stdout.String(), "\n") {
		if strings.Contains(line, activityID) {
			ourLine = line
			break
		}
	}
	s.NotEmpty(ourLine, "expected to find a list row for activity %s", activityID)
	// Bug: list shows Status "Running" for activities the server has scheduled
	// but no worker has picked up.
	s.NotContainsf(strings.ToUpper(ourLine), "RUNNING",
		"list status for an SAA on a task queue with no worker should not be 'Running' — it is scheduled, never started; got line: %s",
		ourLine)
}

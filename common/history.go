package common

import (
	"errors"

	"go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

type (
	DecodedPayloadsHistoryEventIterator struct {
		Iter          sdkclient.HistoryEventIterator
		DataConverter converter.DataConverter
	}
)

func (h DecodedPayloadsHistoryEventIterator) HasNext() bool {
	return h.Iter.HasNext()
}

func (h DecodedPayloadsHistoryEventIterator) Next() (*historypb.HistoryEvent, error) {
	ev, err := h.Iter.Next()
	if err != nil {
		return nil, err
	}
	for _, payload := range getPayloads(ev) {
		var data string
		if err := h.DataConverter.FromPayload(payload, &data); err != nil {
			// TODO (dan): can we detect up-front absence of a payload converter for
			// the encoding, instead of letting it error?
			if errors.Is(err, converter.ErrEncodingIsNotSupported) {
				continue
			}
			return nil, err
		}
		payload.Data = []byte(data)
		payload.Metadata[converter.MetadataEncoding] = nil
	}
	return ev, nil
}

func getPayloads(e *historypb.HistoryEvent) []*common.Payload {
	switch e.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
		return e.GetWorkflowExecutionStartedEventAttributes().Input.Payloads

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		return e.GetWorkflowExecutionCompletedEventAttributes().Result.Payloads

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		return []*common.Payload{e.GetWorkflowExecutionFailedEventAttributes().Failure.EncodedAttributes}

	case enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:
		return []*common.Payload{e.GetWorkflowTaskFailedEventAttributes().Failure.EncodedAttributes}

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
		return e.GetActivityTaskScheduledEventAttributes().Input.Payloads

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
		return []*common.Payload{e.GetActivityTaskStartedEventAttributes().LastFailure.EncodedAttributes}

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		return e.GetActivityTaskCompletedEventAttributes().Result.Payloads

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		return []*common.Payload{e.GetActivityTaskFailedEventAttributes().Failure.EncodedAttributes}

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		return []*common.Payload{e.GetActivityTaskTimedOutEventAttributes().Failure.EncodedAttributes}

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		return e.GetActivityTaskCanceledEventAttributes().Details.Payloads

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return e.GetWorkflowExecutionCanceledEventAttributes().Details.Payloads

	case enumspb.EVENT_TYPE_MARKER_RECORDED:
		return []*common.Payload{e.GetMarkerRecordedEventAttributes().Failure.EncodedAttributes}

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
		return e.GetWorkflowExecutionSignaledEventAttributes().Input.Payloads

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		return e.GetWorkflowExecutionTerminatedEventAttributes().Details.Payloads

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		attrs := e.GetWorkflowExecutionContinuedAsNewEventAttributes()
		return append(attrs.Input.Payloads, attrs.LastCompletionResult.Payloads...)

	case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
		return e.GetStartChildWorkflowExecutionInitiatedEventAttributes().Input.Payloads

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
		return e.GetChildWorkflowExecutionCompletedEventAttributes().Result.Payloads

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
		return []*common.Payload{e.GetChildWorkflowExecutionFailedEventAttributes().Failure.EncodedAttributes}

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
		return e.GetChildWorkflowExecutionCanceledEventAttributes().Details.Payloads

	case enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
		return e.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().Input.Payloads

	default:
		return []*common.Payload{}
	}
}

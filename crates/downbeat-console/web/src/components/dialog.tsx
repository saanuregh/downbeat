// A modal dialog on the native <dialog> element: the browser makes the rest of the page inert
// (so focus stays inside) and closes it on Escape; focus then returns to what opened it.
import {
  useEffect,
  useEffectEvent,
  useId,
  useRef,
  type ReactNode,
} from "react";

export function Dialog(props: {
  title: string;
  onClose: () => void;
  children: ReactNode;
  footer?: ReactNode;
  wide?: boolean;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  const titleId = useId();
  const onCancel = useEffectEvent(() => props.onClose());

  useEffect(() => {
    const dialog = ref.current;
    if (!dialog) return undefined;
    const opener = document.activeElement;
    if (!dialog.open) dialog.showModal();
    const cancel = (event: Event) => {
      event.preventDefault();
      onCancel();
    };
    dialog.addEventListener("cancel", cancel);
    return () => {
      dialog.removeEventListener("cancel", cancel);
      if (dialog.open) dialog.close();
      // React removes the element before this runs, so the browser cannot restore focus itself.
      if (opener instanceof HTMLElement && opener.isConnected) opener.focus();
    };
    // The dialog opens once, when it mounts; onCancel always calls the latest onClose.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <dialog
      ref={ref}
      className={props.wide ? "dialog dialog-wide" : "dialog"}
      aria-labelledby={titleId}
    >
      <div className="dialog-header">
        <h2 id={titleId}>{props.title}</h2>
        <button
          type="button"
          className="button button-quiet"
          onClick={props.onClose}
          aria-label="Close"
        >
          ×
        </button>
      </div>
      <div className="dialog-body">{props.children}</div>
      {props.footer && <div className="dialog-footer">{props.footer}</div>}
    </dialog>
  );
}

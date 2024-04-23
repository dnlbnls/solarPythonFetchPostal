<!-- <Button type="primary/secondary" size="sm/lg" disabled>Button content</Button> -->
<script lang="ts">
  export let type: String | 'primary' | 'secondary' | 'accent' | 'body' | 'error' | 'warning' | 'success' | 'info' | 'help' = 'primary';
  export let size: String | 'sm' | 'lg' | undefined;

  let type_classes = {
    'primary':   'text-white bg-primary-600 hover:bg-primary-700 focus:ring-primary-300',
    'secondary': 'bg-secondary-500 hover:bg-secondary-600 focus:ring-secondary-200',
    'accent' :   'text-white bg-accent-400 hover:bg-accent-500 focus:ring-accent-100',
    'error':     'text-white bg-error-600 hover:bg-error-700 focus:ring-error-300',
    'warning':   'bg-warning-400 hover:bg-warning-500 focus:ring-warning-200',
    'success':   'text-white bg-success-400 hover:bg-success-500 focus:ring-success-200',
    'info':      'text-white bg-info-500 hover:bg-info-600 focus:ring-info-200',
    'help':      'text-white bg-help-700 hover:bg-help-800 focus:ring-help-200'
  }

  let size_classes = {
    'sm': 'text-sm px-4 py-2',
    'lg': 'text-xl px-6 py-3',
  }

  let selectedType = type_classes[type];
  let selectedSize = (size in size_classes) ? size_classes[size] : '';
 
  let clazz: String;
  export {clazz as class};

  let toApply = (clazz ?? '') + ' ' + selectedType + ' ' + selectedSize;

</script>



<button class="{toApply}">
  <slot />
</button>


